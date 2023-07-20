use anyhow::Result;
use tokio::sync::watch;

use super::decoder::*;
use super::transfers_cache::TransferId;
use crate::proto;

pub struct IncomingTransfer {
    buffer: Vec<u8>,
    transfer_id: TransferId,
    max_answer_size: u32,
    confirm_count: usize,
    data: Vec<u8>,
    decoder: Option<RaptorQDecoder>,
    part: u32,
    state: watch::Sender<()>,
    total_size: Option<u32>,
}

impl IncomingTransfer {
    pub fn new(transfer_id: TransferId, max_answer_size: u32) -> Self {
        let (state, _) = watch::channel(());
        Self {
            buffer: Vec::new(),
            transfer_id,
            max_answer_size,
            confirm_count: 0,
            data: Vec::new(),
            decoder: None,
            part: 0,
            state,
            total_size: None,
        }
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.total_size, Some(total_size) if self.data.len() >= total_size as usize)
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    pub fn take_data(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.data)
    }

    pub fn process_chunk(&mut self, message: MessagePart) -> Result<Option<&[u8]>> {
        anyhow::ensure!(
            message.seqno < 16777216,
            IncomingTransferError::InvalidSeqno
        );

        // Check FEC type
        let fec_type = message.fec_type;

        // Initialize `total_size` on first message
        let total_size = match self.total_size {
            Some(total_size) if total_size as u64 != message.total_size => {
                return Err(IncomingTransferError::TotalSizeMismatch.into())
            }
            Some(total_size) => total_size,
            None => {
                if message.total_size > self.max_answer_size as u64 {
                    return Err(IncomingTransferError::TooBigTransferSize.into());
                }
                let total_size = message.total_size as u32;
                self.total_size = Some(total_size);
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
                        transfer_id: &self.transfer_id,
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
                        transfer_id: &self.transfer_id,
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
                        transfer_id: &self.transfer_id,
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

    pub fn state(&self) -> &watch::Sender<()> {
        &self.state
    }
}

pub struct MessagePart {
    pub fec_type: proto::rldp::RaptorQFecType,
    pub part: u32,
    pub total_size: u64,
    pub seqno: u32,
    pub data: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
enum IncomingTransferError {
    #[error("Invalid packet seqno")]
    InvalidSeqno,
    #[error("Total packet size mismatch")]
    TotalSizeMismatch,
    #[error("Packet parameters mismatch")]
    PacketParametersMismatch,
    #[error("Too big size for RLDP transfer")]
    TooBigTransferSize,
}
