use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tl_proto::{TlRead, TlWrite};
use tokio::sync::mpsc;

use self::incoming::*;
use self::outgoing::*;
use super::compression;
use super::NodeOptions;
use crate::adnl;
use crate::proto;
use crate::subscriber::*;
use crate::util::*;

mod incoming;
mod outgoing;

pub struct TransfersHandler {
    state: Arc<TransfersHandlerState>,
}

impl TransfersHandler {
    pub fn new(subscribers: Vec<Arc<dyn QuerySubscriber>>, options: NodeOptions) -> Self {
        Self {
            state: Arc::new(TransfersHandlerState {
                started_at: Instant::now(),
                transfers: Default::default(),
                subscribers,
                options,
            }),
        }
    }

    pub fn gc(&self) {
        let now = self.state.started_at.elapsed().as_millis() as u64;
        self.state.transfers.retain(|_, transfer| {
            !matches!(transfer, RldpTransfer::Done { store_until, .. } if *store_until <= now)
        });
    }

    /// Sends serialized query and waits answer
    #[tracing::instrument(level = "debug", skip(self, adnl, data), err)]
    pub async fn query(
        &self,
        adnl: &Arc<adnl::Node>,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        data: Vec<u8>,
        roundtrip: Option<u64>,
    ) -> Result<(Option<Vec<u8>>, u64)> {
        use futures_util::future::Either;

        let state = self.state.as_ref();

        // Generate transport duplex
        let outgoing_transfer_id = gen_fast_bytes();
        let incoming_transfer_id = negate_id(outgoing_transfer_id);

        let outgoing_transfer = OutgoingTransfer::new(&outgoing_transfer_id, &state.options, data);
        let incoming_transfer = IncomingTransfer::new(&incoming_transfer_id, &state.options);

        state.transfers.insert(
            outgoing_transfer_id,
            RldpTransfer::Outgoing(outgoing_transfer.state().clone()),
        );

        // Insert transfers into map with guards
        let (parts_tx, parts_rx) = mpsc::unbounded_channel();
        state
            .transfers
            .insert(incoming_transfer_id, RldpTransfer::Incoming(parts_tx));

        let mut outgoing_transfer_guard =
            TransferGuard::new(state, &outgoing_transfer_id, TransferType::Outgoing);
        let mut incoming_transfer_guard =
            TransferGuard::new(state, &incoming_transfer_id, TransferType::Incoming);

        // Send data and wait until something is received
        let outgoing_transfer_state = outgoing_transfer.state().clone();

        let receiver = std::pin::pin!(incoming_transfer.receive(
            adnl,
            local_id,
            peer_id,
            parts_rx,
            Some(&outgoing_transfer_state),
        ));
        let sender = std::pin::pin!(outgoing_transfer.send(adnl, local_id, peer_id, roundtrip));

        match futures_util::future::select(receiver, sender).await {
            Either::Left((received, sent)) => {
                incoming_transfer_guard.mark_as_done();
                let data = received?;
                let (_, roundtrip) = sent.await?;
                Ok((data, roundtrip))
            }
            Either::Right((sent, received)) => {
                outgoing_transfer_guard.mark_as_done();
                let (sent, roundtrip) = sent?;
                if !sent {
                    return Ok((None, roundtrip));
                }

                let data = received.await?;
                Ok((data, roundtrip))
            }
        }
    }

    /// Handles incoming message
    #[tracing::instrument(level = "debug", skip(self, adnl), err)]
    pub async fn handle_message(
        &self,
        adnl: &Arc<adnl::Node>,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        message: proto::rldp::MessagePart<'_>,
    ) -> Result<()> {
        match message {
            proto::rldp::MessagePart::MessagePart {
                transfer_id,
                fec_type,
                part,
                total_size,
                seqno,
                data,
            } => loop {
                // Trying to get existing transfer
                if let Some(item) = self.state.transfers.get(transfer_id) {
                    // If transfer exists
                    break match item.value() {
                        // Forward message part on `incoming` state
                        RldpTransfer::Incoming(parts_tx) => {
                            let _ = parts_tx.send(MessagePart {
                                fec_type,
                                part,
                                total_size,
                                seqno,
                                data: data.to_vec(),
                            });
                        }
                        // Blindly confirm receiving packets on finished incoming transfer
                        RldpTransfer::Done {
                            ty: TransferType::Incoming,
                            ..
                        } => {
                            drop(item); // drop item ref to prevent DashMap deadlocks

                            // Send complete message
                            let buffer = tl_proto::serialize(proto::rldp::MessagePart::Complete {
                                transfer_id,
                                part,
                            });
                            ok!(adnl.send_custom_message(local_id, peer_id, &buffer));
                        }
                        // Do nothing for other cases
                        _ => {}
                    };
                }

                // If transfer doesn't exist (it is a query from other node)
                match self
                    .handle_answer(adnl, local_id, peer_id, transfer_id)
                    .await?
                {
                    // Forward message part on `incoming` state (for newly created transfer)
                    Some(parts_tx) => {
                        let _ = parts_tx.send(MessagePart {
                            fec_type,
                            part,
                            total_size,
                            seqno,
                            data: data.to_vec(),
                        });
                        break;
                    }
                    // In case of intermediate state - retry
                    None => continue,
                }
            },
            proto::rldp::MessagePart::Confirm {
                transfer_id,
                part,
                seqno,
            } => {
                if let Some(transfer) = self.state.transfers.get(transfer_id) {
                    if let RldpTransfer::Outgoing(state) = transfer.value() {
                        if *state.part().borrow() == part {
                            state.update_seqno_in(seqno);
                        }
                    }
                }
            }
            proto::rldp::MessagePart::Complete { transfer_id, part } => {
                if let Some(transfer) = self.state.transfers.get(transfer_id) {
                    if let RldpTransfer::Outgoing(state) = transfer.value() {
                        let changed = state.part().send_if_modified(|current_part| {
                            let should_change = *current_part == part;
                            *current_part += should_change as u32;
                            should_change
                        });

                        if changed {
                            state.reset_seqno();
                        }
                    }
                }
            }
        };

        // Done
        Ok(())
    }

    /// Receives incoming query and sends answer
    #[tracing::instrument(level = "debug", skip(self, adnl), err)]
    async fn handle_answer(
        &self,
        adnl: &Arc<adnl::Node>,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        transfer_id: &TransferId,
    ) -> Result<Option<MessagePartsTx>> {
        use dashmap::mapref::entry::Entry;

        struct IncomingTransferGuard {
            state: Arc<TransfersHandlerState>,
            transfer_id: Option<TransferId>,
        }

        impl IncomingTransferGuard {
            fn new(state: Arc<TransfersHandlerState>, transfer_id: TransferId) -> Self {
                Self {
                    state,
                    transfer_id: Some(transfer_id),
                }
            }

            fn mark_as_done(&mut self) {
                if let Some(transfer_id) = self.transfer_id.take() {
                    let store_until = self
                        .state
                        .options
                        .compute_store_until(&self.state.started_at);
                    self.state.transfers.insert(
                        transfer_id,
                        RldpTransfer::Done {
                            ty: TransferType::Incoming,
                            store_until,
                        },
                    );
                }
            }
        }

        impl Drop for IncomingTransferGuard {
            fn drop(&mut self) {
                self.mark_as_done();
            }
        }

        let (parts_tx, parts_rx) = match self.state.transfers.entry(*transfer_id) {
            // Create new transfer
            Entry::Vacant(entry) => {
                let (parts_tx, parts_rx) = mpsc::unbounded_channel();
                entry.insert(RldpTransfer::Incoming(parts_tx.clone()));
                (parts_tx, parts_rx)
            }
            // Or do nothing if it already exists
            Entry::Occupied(_) => return Ok(None),
        };

        // Prepare transfer
        let incoming_transfer_id = *transfer_id;

        // Spawn processing task
        let adnl = adnl.clone();
        let local_id = *local_id;
        let peer_id = *peer_id;
        let state = self.state.clone();
        let mut incoming_transfer_guard =
            IncomingTransferGuard::new(state.clone(), incoming_transfer_id);

        tokio::spawn(async move {
            // Wait until incoming query is received
            let incoming_transfer = IncomingTransfer::new(&incoming_transfer_id, &state.options);

            let data = match incoming_transfer
                .receive(&adnl, &local_id, &peer_id, parts_rx, None)
                .await
            {
                Ok(Some(data)) => data,
                Ok(None) => {
                    return tracing::debug!(
                        %local_id,
                        %peer_id,
                        transfer_id = %DisplayHash(&incoming_transfer_id),
                        "query request receiver timed out"
                    );
                }
                Err(e) => {
                    return tracing::debug!(
                        %local_id,
                        %peer_id,
                        transfer_id = %DisplayHash(&incoming_transfer_id),
                        "failed to receive query request: {e:?}"
                    );
                }
            };
            incoming_transfer_guard.mark_as_done();

            // Deserialize incoming query
            let query = match OwnedRldpMessageQuery::from_data(data) {
                Some(query) => {
                    tracing::debug!(
                        %local_id,
                        %peer_id,
                        transfer_id = %DisplayHash(&incoming_transfer_id),
                        "received query"
                    );
                    query
                }
                None => {
                    return tracing::debug!(
                        %local_id,
                        %peer_id,
                        transfer_id = %DisplayHash(&incoming_transfer_id),
                        "received unknown query message"
                    );
                }
            };

            let ctx = SubscriberContext {
                adnl: &adnl,
                local_id: &local_id,
                peer_id: &peer_id,
            };
            let answer =
                match process_rldp_query(ctx, &state.subscribers, query, &state.options).await {
                    Ok(QueryProcessingResult::Processed(answer)) => answer,
                    Ok(QueryProcessingResult::Rejected) => {
                        return tracing::debug!(
                            %local_id,
                            %peer_id,
                            transfer_id = %DisplayHash(&incoming_transfer_id),
                            "rejected query"
                        );
                    }
                    Err(e) => {
                        return tracing::debug!(
                            %local_id,
                            %peer_id,
                            transfer_id = %DisplayHash(&incoming_transfer_id),
                            "failed to process query: {e:?}"
                        );
                    }
                };

            tracing::debug!(
                %local_id,
                %peer_id,
                transfer_id = %DisplayHash(&incoming_transfer_id),
                has_answer = answer.is_some(),
                "processed query"
            );

            if let Some(answer) = answer {
                let outgoing_transfer_id = negate_id(incoming_transfer_id);
                let outgoing_transfer =
                    OutgoingTransfer::new(&outgoing_transfer_id, &state.options, answer);
                let _guard =
                    TransferGuard::new(&state, &outgoing_transfer_id, TransferType::Outgoing);

                state.transfers.insert(
                    outgoing_transfer_id,
                    RldpTransfer::Outgoing(outgoing_transfer.state().clone()),
                );

                // Send answer
                if let Err(e) = outgoing_transfer
                    .send(&adnl, &local_id, &peer_id, None)
                    .await
                {
                    tracing::debug!(
                        %local_id,
                        %peer_id,
                        transfer_id = %DisplayHash(&incoming_transfer_id),
                        "failed to send query answer: {e:?}"
                    );
                }
            }
        });

        // Done
        Ok(Some(parts_tx))
    }
}

pub struct TransfersHandlerState {
    started_at: Instant,
    transfers: TransfersMap,
    subscribers: Vec<Arc<dyn QuerySubscriber>>,
    options: NodeOptions,
}

enum RldpTransfer {
    Incoming(MessagePartsTx),
    Outgoing(Arc<OutgoingTransferState>),
    Done { ty: TransferType, store_until: u64 },
}

impl NodeOptions {
    /// Updates provided roundtrip and returns timeout
    fn update_roundtrip(&self, time: &Instant, roundtrip: &mut u64) -> Duration {
        let elapsed = time.elapsed().as_millis() as u64;

        *roundtrip = if *roundtrip == 0 {
            elapsed
        } else {
            (*roundtrip + elapsed) / 2
        };
        self.compute_timeout(Some(*roundtrip))
    }

    /// Clamps roundtrip to get valid timeout
    fn compute_timeout(&self, roundtrip: Option<u64>) -> Duration {
        Duration::from_millis(match roundtrip {
            Some(roundtrip) if roundtrip > self.query_max_timeout_ms => self.query_max_timeout_ms,
            Some(roundtrip) => std::cmp::max(roundtrip, self.query_min_timeout_ms),
            None => self.query_max_timeout_ms,
        })
    }

    fn compute_store_until(&self, time: &Instant) -> u64 {
        let now = time.elapsed().as_millis() as u64;
        now + self.query_max_timeout_ms * 2
    }
}

async fn process_rldp_query(
    ctx: SubscriberContext<'_>,
    subscribers: &[Arc<dyn QuerySubscriber>],
    mut query: OwnedRldpMessageQuery,
    options: &NodeOptions,
) -> Result<QueryProcessingResult<Vec<u8>>> {
    let answer_compression = match compression::decompress(&query.data) {
        Some(decompressed) => {
            query.data = decompressed;
            true
        }
        None => options.force_compression,
    };

    match process_query(ctx, subscribers, Cow::Owned(query.data)).await? {
        QueryProcessingResult::Processed(answer) => Ok(match answer {
            Some(mut answer) => {
                if answer_compression {
                    if let Err(e) = compression::compress(&mut answer) {
                        tracing::warn!("failed to compress RLDP answer: {e:?}");
                    }
                }
                if answer.len() > query.max_answer_size as usize {
                    return Err(TransfersHandlerError::AnswerSizeExceeded.into());
                }

                QueryProcessingResult::Processed(Some(tl_proto::serialize(
                    proto::rldp::Message::Answer {
                        query_id: &query.query_id,
                        data: &answer,
                    },
                )))
            }
            None => QueryProcessingResult::Processed(None),
        }),
        _ => Ok(QueryProcessingResult::Rejected),
    }
}

struct OwnedRldpMessageQuery {
    query_id: [u8; 32],
    max_answer_size: u64,
    data: Vec<u8>,
}

impl OwnedRldpMessageQuery {
    fn from_data(mut data: Vec<u8>) -> Option<Self> {
        #[derive(TlRead, TlWrite)]
        #[tl(boxed, id = "rldp.query", scheme = "scheme.tl")]
        struct Query {
            #[tl(size_hint = 32)]
            query_id: [u8; 32],
            max_answer_size: u64,
            timeout: u32,
        }

        let mut offset = 0;
        let params = match Query::read_from(&data, &mut offset) {
            Ok(params) => params,
            Err(_) => return None,
        };

        match tl_proto::BytesMeta::read_from(&data, &mut offset) {
            Ok(data_meta) => {
                // SAFETY: parsed `BytesMeta` ensures that remaining packet data contains
                // `data_meta.prefix_len + data_meta.len + data_meta.padding` bytes
                unsafe {
                    std::ptr::copy(
                        data.as_ptr().add(offset + data_meta.prefix_len),
                        data.as_mut_ptr(),
                        data_meta.len,
                    );
                    data.set_len(data_meta.len);
                };
            }
            Err(_) => return None,
        };

        Some(Self {
            query_id: params.query_id,
            max_answer_size: params.max_answer_size,
            data,
        })
    }
}

fn negate_id(id: [u8; 32]) -> [u8; 32] {
    id.map(|item| item ^ 0xff)
}

struct TransferGuard<'a> {
    state: &'a TransfersHandlerState,
    transfer_id: Option<&'a TransferId>,
    ty: TransferType,
}

impl<'a> TransferGuard<'a> {
    fn new(
        state: &'a TransfersHandlerState,
        transfer_id: &'a TransferId,
        ty: TransferType,
    ) -> Self {
        Self {
            state,
            transfer_id: Some(transfer_id),
            ty,
        }
    }

    fn mark_as_done(&mut self) {
        if let Some(transfer_id) = self.transfer_id.take() {
            let store_until = self
                .state
                .options
                .compute_store_until(&self.state.started_at);
            self.state.transfers.insert(
                *transfer_id,
                RldpTransfer::Done {
                    ty: self.ty,
                    store_until,
                },
            );
        }
    }
}

impl Drop for TransferGuard<'_> {
    fn drop(&mut self) {
        self.mark_as_done();
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum TransferType {
    Incoming,
    Outgoing,
}

type MessagePartsTx = mpsc::UnboundedSender<MessagePart>;
type TransfersMap = FastDashMap<TransferId, RldpTransfer>;

type TransferId = [u8; 32];

#[derive(thiserror::Error, Debug)]
enum TransfersHandlerError {
    #[error("Answer size exceeded")]
    AnswerSizeExceeded,
}
