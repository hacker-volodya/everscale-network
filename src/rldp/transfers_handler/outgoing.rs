use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::watch;

use super::TransferId;
use crate::adnl;
use crate::proto;
use crate::rldp::encoder::RaptorQEncoder;
use crate::rldp::NodeOptions;

pub struct OutgoingTransfer<'a> {
    transfer_id: &'a TransferId,
    options: &'a NodeOptions,
    buffer: Vec<u8>,
    data: Vec<u8>,
    current_message_part: u32,
    encoder: Option<RaptorQEncoder>,
    state: Arc<OutgoingTransferState>,
}

impl<'a> OutgoingTransfer<'a> {
    pub fn new(transfer_id: &'a TransferId, options: &'a NodeOptions, data: Vec<u8>) -> Self {
        Self {
            transfer_id,
            options,
            buffer: Vec::new(),
            data,
            current_message_part: 0,
            encoder: None,
            state: Default::default(),
        }
    }

    pub fn state(&self) -> &Arc<OutgoingTransferState> {
        &self.state
    }

    pub async fn send(
        mut self,
        adnl: &adnl::Node,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        roundtrip: Option<u64>,
    ) -> Result<(bool, u64)> {
        // Prepare timeout
        let mut timeout = self.options.compute_timeout(roundtrip);
        let mut roundtrip = roundtrip.unwrap_or_default();

        let waves_interval = Duration::from_millis(self.options.query_wave_interval_ms);

        let mut completed_part = self.state.part().subscribe();

        // For each outgoing message part
        while let Some(packet_count) = self.start_next_part() {
            let wave_len = std::cmp::min(packet_count, self.options.query_wave_len);

            let part = *self.state.part().borrow();

            let mut start = Instant::now();

            let mut incoming_seqno = 0;
            'part: loop {
                // Send parts in waves
                for _ in 0..wave_len {
                    ok!(adnl.send_custom_message(local_id, peer_id, ok!(self.prepare_chunk())));
                }

                if tokio::time::timeout(waves_interval, completed_part.changed())
                    .await
                    .is_ok()
                    && self.is_finished_or_next_part(part)?
                {
                    break 'part;
                }

                // Update timeout on incoming packets
                let new_incoming_seqno = self.state.seqno_in();
                if new_incoming_seqno > incoming_seqno {
                    timeout = self.options.update_roundtrip(&start, &mut roundtrip);
                    incoming_seqno = new_incoming_seqno;
                    start = Instant::now();
                } else if is_timed_out(&start, timeout, incoming_seqno) {
                    return Ok((false, big_roundtrip(self.options, roundtrip)));
                }
            }

            // Update timeout
            timeout = self.options.update_roundtrip(&start, &mut roundtrip);
        }

        // Done
        Ok((true, roundtrip))
    }

    /// Encodes next part of the message. Returns packet count which is required to be sent.
    fn start_next_part(&mut self) -> Option<u32> {
        if self.is_finished() {
            return None;
        }

        let total = self.data.len();
        let part = *self.state.part().borrow() as usize;
        let processed = part * RaptorQEncoder::SLICE;
        if processed >= total {
            return None;
        }

        self.current_message_part = part as u32;

        let chunk_size = std::cmp::min(total - processed, RaptorQEncoder::SLICE);
        let data = &self.data[processed..processed + chunk_size];

        let packet_count = match &mut self.encoder {
            Some(encoder) => {
                encoder.reset(data);
                encoder.params().packet_count
            }
            None => {
                let encoder = self.encoder.insert(RaptorQEncoder::with_data(data));
                encoder.params().packet_count
            }
        };

        if packet_count > 0 {
            Some(packet_count)
        } else {
            None
        }
    }

    fn prepare_chunk(&mut self) -> Result<&[u8]> {
        let encoder = match &mut self.encoder {
            Some(encoder) => encoder,
            None => return Err(OutgoingTransferError::EncoderIsNotReady.into()),
        };

        let mut seqno_out = self.state.seqno_out();
        let previous_seqno_out = seqno_out;

        let data = encoder.encode(&mut seqno_out);

        let seqno_in = self.state.seqno_in();

        let mut next_seqno_out = seqno_out;
        if seqno_out.wrapping_sub(seqno_in) <= WINDOW {
            if previous_seqno_out == seqno_out {
                next_seqno_out += 1;
            }
            self.state.set_seqno_out(next_seqno_out);
        }

        tl_proto::serialize_into(
            proto::rldp::MessagePart::MessagePart {
                transfer_id: self.transfer_id,
                fec_type: *encoder.params(),
                part: self.current_message_part,
                total_size: self.data.len() as u64,
                seqno: seqno_out,
                data: &data,
            },
            &mut self.buffer,
        );
        Ok(&self.buffer)
    }

    fn is_finished(&self) -> bool {
        self.state.has_reply() && {
            (*self.state.part().borrow() as usize + 1) * RaptorQEncoder::SLICE >= self.data.len()
        }
    }

    fn is_finished_or_next_part(&self, part: u32) -> Result<bool> {
        let last_part = *self.state.part().borrow();

        if self.state.has_reply()
            && (last_part as usize + 1) * RaptorQEncoder::SLICE >= self.data.len()
        {
            Ok(true)
        } else {
            match last_part {
                x if x == part => Ok(false),
                x if x == part + 1 => Ok(true),
                _ => Err(OutgoingTransferError::PartMismatch.into()),
            }
        }
    }
}

pub struct OutgoingTransferState {
    part: watch::Sender<u32>,
    has_reply: AtomicBool,
    seqno_out: AtomicU32,
    seqno_in: AtomicU32,
}

impl Default for OutgoingTransferState {
    fn default() -> Self {
        let (part, _) = watch::channel(0);
        Self {
            part,
            has_reply: Default::default(),
            seqno_out: Default::default(),
            seqno_in: Default::default(),
        }
    }
}

impl OutgoingTransferState {
    pub fn part(&self) -> &watch::Sender<u32> {
        &self.part
    }

    pub fn has_reply(&self) -> bool {
        self.has_reply.load(Ordering::Acquire)
    }

    pub fn set_has_reply(&self) {
        self.has_reply.store(true, Ordering::Release);
    }

    pub fn seqno_out(&self) -> u32 {
        self.seqno_out.load(Ordering::Acquire)
    }

    pub fn set_seqno_out(&self, seqno: u32) {
        self.seqno_out.store(seqno, Ordering::Release);
    }

    pub fn seqno_in(&self) -> u32 {
        self.seqno_in.load(Ordering::Acquire)
    }

    pub fn update_seqno_in(&self, seqno: u32) {
        if seqno > self.seqno_out() {
            // Only use seqno which has already been used
            return;
        }

        // Increase seqno
        let seqno_in = self.seqno_in();
        if seqno > seqno_in {
            // NOTE: `compare_exchange` is used here instead of `fetch_max`,
            // becase we only need this value to change, and there is no
            // need to ensure that this value is the maximum known at the time
            self.seqno_in
                .compare_exchange(seqno_in, seqno, Ordering::Release, Ordering::Relaxed)
                .ok();
        }
    }

    pub fn reset_seqno(&self) {
        self.seqno_in.store(0, Ordering::Release);
        self.seqno_out.store(0, Ordering::Release);
    }
}

const WINDOW: u32 = 1000;

fn is_timed_out(time: &Instant, timeout: Duration, updates: u32) -> bool {
    let timeout = timeout.as_millis() as u64;
    time.elapsed().as_millis() as u64 > timeout + timeout * (updates as u64) / 100
}

/// Computes roundtrip for invalid query
fn big_roundtrip(options: &NodeOptions, roundtrip: u64) -> u64 {
    std::cmp::min(roundtrip * 2, options.query_max_timeout_ms)
}

#[derive(thiserror::Error, Debug)]
enum OutgoingTransferError {
    #[error("Encoder is not ready")]
    EncoderIsNotReady,
    #[error("Part mismatch")]
    PartMismatch,
}
