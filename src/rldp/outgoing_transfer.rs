use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::watch;

use super::encoder::*;
use super::transfers_cache::TransferId;
use crate::proto;
use crate::util::*;

pub struct OutgoingTransfer {
    buffer: Vec<u8>,
    transfer_id: TransferId,
    data: Vec<u8>,
    current_message_part: u32,
    encoder: Option<RaptorQEncoder>,
    state: Arc<OutgoingTransferState>,
}

impl OutgoingTransfer {
    pub fn new(data: Vec<u8>, transfer_id: Option<TransferId>) -> Self {
        let transfer_id = transfer_id.unwrap_or_else(gen_fast_bytes);

        Self {
            buffer: Vec::new(),
            transfer_id,
            data,
            current_message_part: 0,
            encoder: None,
            state: Default::default(),
        }
    }

    #[inline(always)]
    pub fn transfer_id(&self) -> &TransferId {
        &self.transfer_id
    }

    /// Encodes next part of the message. Returns packet count which is required to be sent.
    pub fn start_next_part(&mut self) -> Result<Option<u32>> {
        if self.is_finished() {
            return Ok(None);
        }

        let total = self.data.len();
        let part = *self.state.part().borrow() as usize;
        let processed = part * SLICE;
        if processed >= total {
            return Ok(None);
        }

        self.current_message_part = part as u32;

        let chunk_size = std::cmp::min(total - processed, SLICE);
        let encoder = self.encoder.insert(RaptorQEncoder::with_data(
            &self.data[processed..processed + chunk_size],
        ));

        let packet_count = encoder.params().packet_count;
        Ok(if packet_count > 0 {
            Some(packet_count)
        } else {
            None
        })
    }

    pub fn prepare_chunk(&mut self) -> Result<&[u8]> {
        let encoder = match &mut self.encoder {
            Some(encoder) => encoder,
            None => return Err(OutgoingTransferError::EncoderIsNotReady.into()),
        };

        let mut seqno_out = self.state.seqno_out();
        let previous_seqno_out = seqno_out;

        let data = ok!(encoder.encode(&mut seqno_out));

        let seqno_in = self.state.seqno_in();

        let mut next_seqno_out = seqno_out;
        if seqno_out - seqno_in <= WINDOW {
            if previous_seqno_out == seqno_out {
                next_seqno_out += 1;
            }
            self.state.set_seqno_out(next_seqno_out);
        }

        // tracing::info!(
        //     part = self.current_message_part,
        //     seqno_in,
        //     seqno_out,
        //     next_seqno_out
        // );

        tl_proto::serialize_into(
            proto::rldp::MessagePart::MessagePart {
                transfer_id: &self.transfer_id,
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

    pub fn is_finished(&self) -> bool {
        self.state.has_reply() && {
            (*self.state.part().borrow() as usize + 1) * SLICE >= self.data.len()
        }
    }

    pub fn is_finished_or_next_part(&self, part: u32) -> Result<bool> {
        let last_part = *self.state.part().borrow();

        if self.state.has_reply() && (last_part as usize + 1) * SLICE >= self.data.len() {
            Ok(true)
        } else {
            match last_part {
                x if x == part => Ok(false),
                x if x == part + 1 => Ok(true),
                _ => Err(OutgoingTransferError::PartMismatch.into()),
            }
        }
    }

    pub fn state(&self) -> &Arc<OutgoingTransferState> {
        &self.state
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

    pub fn set_reply(&self) {
        self.has_reply.store(true, Ordering::Release);
    }

    pub fn seqno_out(&self) -> u32 {
        self.seqno_out.load(Ordering::Acquire)
    }

    pub fn set_seqno_out(&self, seqno: u32) {
        self.seqno_out.fetch_max(seqno, Ordering::Release);
    }

    pub fn seqno_in(&self) -> u32 {
        self.seqno_in.load(Ordering::Acquire)
    }

    pub fn set_seqno_in(&self, seqno: u32) {
        if seqno > self.seqno_out() {
            return;
        }
        self.seqno_in.fetch_max(seqno, Ordering::Release);
    }

    pub fn reset_seqno(&self) {
        self.seqno_in.store(0, Ordering::Release);
        self.seqno_out.store(0, Ordering::Release);
    }
}

const WINDOW: u32 = 1000;
const SLICE: usize = 2000000;

#[derive(thiserror::Error, Debug)]
enum OutgoingTransferError {
    #[error("Encoder is not ready")]
    EncoderIsNotReady,
    #[error("Part mismatch")]
    PartMismatch,
}
