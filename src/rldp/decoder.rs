use everscale_raptorq::{Decoder, EncodingPacket, ObjectTransmissionInformation, PayloadId};

use crate::proto::rldp::RaptorQFecType;

pub struct RaptorQDecoder {
    engine: Decoder,
    params: RaptorQFecType,
}

impl RaptorQDecoder {
    pub fn with_params(params: RaptorQFecType) -> Self {
        Self {
            engine: Decoder::new(ObjectTransmissionInformation::with_defaults(
                params.total_len as u64,
                params.packet_len as u16,
            )),
            params,
        }
    }

    pub fn decode(&mut self, seqno: u32, data: Vec<u8>) -> Option<Vec<u8>> {
        let packet = EncodingPacket::new(PayloadId::new(0, seqno), data);
        self.engine.decode(packet)
    }

    pub fn params(&self) -> &RaptorQFecType {
        &self.params
    }
}
