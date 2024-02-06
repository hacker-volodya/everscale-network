use everscale_raptorq::{
    ObjectTransmissionInformation, SourceBlockEncoder, SourceBlockEncodingPlan,
};

use crate::proto::rldp::RaptorQFecType;

pub struct RaptorQEncoderConstraints {
    pub max_data_size: usize,
    pub packet_len: u32,
}

pub struct RaptorQEncoder {
    params: RaptorQFecType,
    encoder: SourceBlockEncoder,
    cached_plan: Option<SourceBlockEncodingPlan>,
}

impl RaptorQEncoder {
    pub const SLICE: usize = 2000000;
    pub const MAX_TRANSMISSION_UNIT: u32 = 768;

    pub fn check_fec_type(
        fec_type: &RaptorQFecType,
        constraints: RaptorQEncoderConstraints,
    ) -> bool {
        fec_type.packet_len == constraints.packet_len
            && fec_type.total_len > 0
            && fec_type.total_len as usize <= constraints.max_data_size
    }

    pub fn with_data(data: &[u8]) -> Self {
        let mut cached_plan = None;
        let encoder = make_encoder(data, &mut cached_plan);

        Self {
            params: RaptorQFecType {
                total_len: data.len() as u32,
                packet_len: Self::MAX_TRANSMISSION_UNIT,
                packet_count: encoder.source_symbols_len(),
            },
            encoder,
            cached_plan,
        }
    }

    pub fn reset(&mut self, data: &[u8]) {
        self.encoder = make_encoder(data, &mut self.cached_plan);
        self.params = RaptorQFecType {
            total_len: data.len() as u32,
            packet_len: Self::MAX_TRANSMISSION_UNIT,
            packet_count: self.encoder.source_symbols_len(),
        };
    }

    pub fn encode(&mut self, seqno: &mut u32) -> Vec<u8> {
        let (new_seqno, data) = if let Some((seqno, symbol)) = self.encoder.take_source_packet() {
            (seqno, symbol.into_bytes())
        } else {
            let (payload_id, data) = self.encoder.repair_packet(*seqno).split();
            (payload_id.encoding_symbol_id(), data)
        };

        *seqno = new_seqno;
        data
    }

    #[inline(always)]
    pub fn params(&self) -> &RaptorQFecType {
        &self.params
    }
}

fn make_encoder(data: &[u8], plan: &mut Option<SourceBlockEncodingPlan>) -> SourceBlockEncoder {
    let config = ObjectTransmissionInformation::with_defaults(
        data.len() as u64,
        RaptorQEncoder::MAX_TRANSMISSION_UNIT as u16,
    );

    let symbol_count = int_div_ceil(
        config.transfer_length(),
        RaptorQEncoder::MAX_TRANSMISSION_UNIT as u64,
    ) as u16;

    let unaligned_data_len = data.len();
    let aligned_data_len = symbol_count as usize * RaptorQEncoder::MAX_TRANSMISSION_UNIT as usize;

    let encoder = {
        let plan = 'plan: {
            if let Some(plan) = plan {
                if plan.source_symbol_count() == symbol_count {
                    break 'plan plan;
                }
            }
            plan.insert(SourceBlockEncodingPlan::generate(symbol_count))
        };

        let mut padded;
        let data = if aligned_data_len != unaligned_data_len {
            padded = Vec::with_capacity(aligned_data_len);
            padded.extend_from_slice(data);
            padded.resize(aligned_data_len, 0);
            &padded
        } else {
            data
        };

        SourceBlockEncoder::with_encoding_plan2(0, &config, data, plan)
    };

    if unaligned_data_len < RaptorQEncoder::SLICE {
        *plan = None;
    }

    encoder
}

const fn int_div_ceil(num: u64, denom: u64) -> u32 {
    if num % denom == 0 {
        (num / denom) as u32
    } else {
        (num / denom + 1) as u32
    }
}
