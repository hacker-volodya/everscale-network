use std::num::NonZeroU32;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;

use super::{OutgoingTransferState, TransferId};
use crate::adnl;
use crate::proto;
use crate::rldp::decoder::RaptorQDecoder;
use crate::rldp::NodeOptions;

pub struct IncomingTransfer<'a> {
    transfer_id: &'a TransferId,
    options: &'a NodeOptions,
    buffer: Vec<u8>,
    confirm_count: u32,
    data: Vec<u8>,
    decoder: Option<RaptorQDecoder>,
    part: u32,
    total_size: Option<NonZeroU32>,
}

impl<'a> IncomingTransfer<'a> {
    pub fn new(transfer_id: &'a TransferId, options: &'a NodeOptions) -> Self {
        Self {
            transfer_id,
            options,
            buffer: Vec::new(),
            confirm_count: 0,
            data: Vec::new(),
            decoder: None,
            part: 0,
            total_size: None,
        }
    }

    #[tracing::instrument(level = "debug", skip(self, adnl, rx, outgoing_transfer_state), err)]
    pub async fn receive(
        mut self,
        adnl: &adnl::Node,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        mut rx: MessagePartsRx,
        mut outgoing_transfer_state: Option<&OutgoingTransferState>,
    ) -> Result<Option<Vec<u8>>> {
        let mut timeout = self.options.compute_timeout(None);
        let mut roundtrip = 0;

        // For each incoming message part
        loop {
            let start = Instant::now();
            tracing::trace!(?timeout, "waiting for answer");
            let message = match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(_) => return Ok(None),
            };

            timeout = self.options.update_roundtrip(&start, &mut roundtrip);

            // Try to process its data
            if let Some(reply) = self.process_chunk(message)? {
                // If some data was successfully processed
                // Send `complete` or `confirm` message as reply
                adnl.send_custom_message(local_id, peer_id, reply)?;
            }

            // Notify state, that some reply was received
            if let Some(outgoing_transfer_state) = outgoing_transfer_state.take() {
                outgoing_transfer_state.set_has_reply();
            }

            // Exit loop if all bytes were received
            if let Some(total_size) = self.total_size {
                if self.data.len() >= total_size.get() as usize {
                    return Ok(Some(self.data));
                }
            }
        }

        Err(IncomingTransferError::ReceiverCancelled.into())
    }

    fn process_chunk(&mut self, message: MessagePart) -> Result<Option<&[u8]>> {
        // Check FEC type
        let fec_type = message.fec_type;

        // Initialize `total_size` on first message
        let total_size = match self.total_size {
            Some(total_size) => {
                let total_size = total_size.get();
                if message.total_size != total_size as u64 {
                    return Err(IncomingTransferError::TotalSizeMismatch.into());
                }

                total_size
            }
            None => {
                if message.total_size > self.options.max_answer_size as u64 {
                    return Err(IncomingTransferError::TooBigTransferSize.into());
                }

                let total_size = match NonZeroU32::new(message.total_size as u32) {
                    Some(total_size) => self.total_size.insert(total_size).get(),
                    None => return Err(IncomingTransferError::TooSmallTransferSize.into()),
                };
                self.data = Vec::with_capacity(total_size as usize);
                total_size
            }
        };

        // Check message part
        let decoder = match message.part.cmp(&self.part) {
            std::cmp::Ordering::Equal => match &mut self.decoder {
                Some(decoder) if decoder.params() != &fec_type => {
                    return Err(IncomingTransferError::PacketParametersMismatch.into())
                }
                Some(decoder) => decoder,
                None => self
                    .decoder
                    .get_or_insert_with(|| RaptorQDecoder::with_params(fec_type)),
            },
            std::cmp::Ordering::Less => {
                tl_proto::serialize_into(
                    proto::rldp::MessagePart::Complete {
                        transfer_id: self.transfer_id,
                        part: message.part,
                    },
                    &mut self.buffer,
                );
                return Ok(Some(&self.buffer));
            }
            std::cmp::Ordering::Greater => return Ok(None),
        };

        // Decode message data
        match decoder.decode(message.seqno, message.data) {
            Some(data) if data.len() + self.data.len() > total_size as usize => {
                Err(IncomingTransferError::TooBigTransferSize.into())
            }
            Some(mut data) => {
                self.data.append(&mut data);

                // Reset decoder
                if self.data.len() < total_size as usize {
                    self.decoder = None;
                    self.part += 1;
                    self.confirm_count = 0;
                }

                tl_proto::serialize_into(
                    proto::rldp::MessagePart::Complete {
                        transfer_id: self.transfer_id,
                        part: message.part,
                    },
                    &mut self.buffer,
                );
                Ok(Some(&self.buffer))
            }
            None if self.confirm_count == 9 => {
                self.confirm_count = 0;

                tl_proto::serialize_into(
                    proto::rldp::MessagePart::Confirm {
                        transfer_id: self.transfer_id,
                        part: message.part,
                        seqno: message.seqno,
                    },
                    &mut self.buffer,
                );
                Ok(Some(&self.buffer))
            }
            None => {
                self.confirm_count += 1;
                Ok(None)
            }
        }
    }
}

pub type MessagePartsRx = mpsc::UnboundedReceiver<MessagePart>;

pub struct MessagePart {
    pub fec_type: proto::rldp::RaptorQFecType,
    pub part: u32,
    pub total_size: u64,
    pub seqno: u32,
    pub data: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
enum IncomingTransferError {
    #[error("Total packet size mismatch")]
    TotalSizeMismatch,
    #[error("Packet parameters mismatch")]
    PacketParametersMismatch,
    #[error("Too big size for RLDP transfer")]
    TooBigTransferSize,
    #[error("Too small size for RLDP transfer")]
    TooSmallTransferSize,
    #[error("Receiver cancelled")]
    ReceiverCancelled,
}
