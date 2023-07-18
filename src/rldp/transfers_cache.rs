use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tl_proto::{TlRead, TlWrite};
use tokio::sync::{mpsc, oneshot};

use super::compression;
use super::incoming_transfer::*;
use super::outgoing_transfer::*;
use super::NodeOptions;
use crate::adnl;
use crate::proto;
use crate::subscriber::*;
use crate::util::*;

pub struct TransfersCache {
    transfers: Arc<FastDashMap<TransferId, RldpTransfer>>,
    subscribers: Arc<Vec<Arc<dyn QuerySubscriber>>>,
    query_options: QueryOptions,
    max_answer_size: u32,
    force_compression: bool,
}

impl TransfersCache {
    pub fn new(subscribers: Vec<Arc<dyn QuerySubscriber>>, options: NodeOptions) -> Self {
        Self {
            transfers: Arc::new(Default::default()),
            subscribers: Arc::new(subscribers),
            query_options: QueryOptions {
                query_wave_len: options.query_wave_len,
                query_wave_interval_ms: options.query_wave_interval_ms,
                query_min_timeout_ms: options.query_min_timeout_ms,
                query_max_timeout_ms: options.query_max_timeout_ms,
            },
            max_answer_size: options.max_answer_size,
            force_compression: options.force_compression,
        }
    }

    /// Sends serialized query and waits answer
    pub async fn query(
        &self,
        adnl: &Arc<adnl::Node>,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        data: Vec<u8>,
        roundtrip: Option<u64>,
    ) -> Result<(Option<Vec<u8>>, u64)> {
        use futures_util::future::Either;

        // Initiate outgoing transfer with new id
        let outgoing_transfer = OutgoingTransfer::new(data, None);
        let outgoing_transfer_id = *outgoing_transfer.transfer_id();
        let outgoing_transfer_state = outgoing_transfer.state().clone();
        self.transfers.insert(
            outgoing_transfer_id,
            RldpTransfer::Outgoing(outgoing_transfer_state.clone()),
        );

        // Initiate incoming transfer with derived id
        let incoming_transfer_id = negate_id(outgoing_transfer_id);
        let incoming_transfer = IncomingTransfer::new(incoming_transfer_id, self.max_answer_size);
        let mut incoming_transfer_state = incoming_transfer.state().subscribe();
        let (parts_tx, parts_rx) = mpsc::unbounded_channel();
        self.transfers
            .insert(incoming_transfer_id, RldpTransfer::Incoming(parts_tx));

        // Prepare contexts
        let outgoing_context = OutgoingContext {
            adnl: adnl.clone(),
            local_id: *local_id,
            peer_id: *peer_id,
            transfer: outgoing_transfer,
        };

        let mut incoming_context = IncomingContext {
            adnl: adnl.clone(),
            local_id: *local_id,
            peer_id: *peer_id,
            transfer: incoming_transfer,
            transfer_id: outgoing_transfer_id,
        };

        // Start query transfer loop
        let (res_tx, res_rx) = oneshot::channel();

        // Spawn receiver
        tokio::spawn(async move {
            incoming_context
                .receive(Some(outgoing_transfer_state), parts_rx)
                .await;
            res_tx.send(incoming_context.transfer).ok();
        });

        // Send data and wait until something is received
        let result = outgoing_context.send(self.query_options, roundtrip).await;
        if result.is_ok() {
            self.transfers
                .insert(outgoing_transfer_id, RldpTransfer::Done);
        }

        let result = match result {
            Ok((true, mut roundtrip)) => {
                let mut res_rx = std::pin::pin!(res_rx);
                let mut timeout = self.query_options.compute_timeout(Some(roundtrip));

                let mut start = Instant::now();
                loop {
                    let updated = std::pin::pin!(tokio::time::timeout(
                        Duration::from_millis(timeout),
                        incoming_transfer_state.changed(),
                    ));

                    match futures_util::future::select(&mut res_rx, updated).await {
                        Either::Left((reply, _)) => {
                            break Ok((Some(reply?.into_data()), roundtrip));
                        }
                        Either::Right((Ok(state), _)) => {
                            if state.is_ok() {
                                timeout =
                                    self.query_options.update_roundtrip(&mut roundtrip, &start);
                                start = Instant::now();
                            }
                        }
                        Either::Right((Err(_), _)) => {
                            // Stop polling on timeout
                            break Ok((None, roundtrip));
                        }
                    }
                }
            }
            Ok((false, roundtrip)) => Ok((None, roundtrip)),
            Err(e) => {
                // Reset transfer entries
                self.transfers
                    .insert(outgoing_transfer_id, RldpTransfer::Done);
                Err(e)
            }
        };

        self.transfers
            .insert(incoming_transfer_id, RldpTransfer::Done);

        // Clear transfers in background
        tokio::spawn({
            let transfers = self.transfers.clone();
            let interval = self.query_options.completion_interval();
            async move {
                tokio::time::sleep(interval).await;
                transfers.remove(&outgoing_transfer_id);
                transfers.remove(&incoming_transfer_id);
            }
        });

        // Done
        result
    }

    pub fn len(&self) -> usize {
        self.transfers.len()
    }

    /// Handles incoming message
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
                match self.transfers.get(transfer_id) {
                    // If transfer exists
                    Some(item) => match item.value() {
                        // Forward message part on `incoming` state
                        RldpTransfer::Incoming(parts_tx) => {
                            let _ = parts_tx.send(MessagePart {
                                fec_type,
                                part,
                                total_size,
                                seqno,
                                data: data.to_vec(),
                            });
                            break;
                        }
                        // Blindly confirm receiving in case of other states
                        _ => {
                            drop(item); // drop item ref to prevent DashMap deadlocks

                            // Send confirm message
                            let mut buffer = Vec::with_capacity(44);
                            proto::rldp::MessagePart::Confirm {
                                transfer_id,
                                part,
                                seqno,
                            }
                            .write_to(&mut buffer);
                            ok!(adnl.send_custom_message(local_id, peer_id, &buffer));

                            // Send complete message
                            buffer.clear();
                            proto::rldp::MessagePart::Complete { transfer_id, part }
                                .write_to(&mut buffer);
                            ok!(adnl.send_custom_message(local_id, peer_id, &buffer));

                            // Done
                            break;
                        }
                    },
                    // If transfer doesn't exist (it is a query from other node)
                    None => match self
                        .create_answer_handler(adnl, local_id, peer_id, *transfer_id)
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
                    },
                }
            },
            proto::rldp::MessagePart::Confirm {
                transfer_id,
                part,
                seqno,
            } => {
                if let Some(transfer) = self.transfers.get(transfer_id) {
                    if let RldpTransfer::Outgoing(state) = transfer.value() {
                        if *state.part().borrow() == part {
                            state.set_seqno_in(seqno);
                        }
                    }
                }
            }
            proto::rldp::MessagePart::Complete { transfer_id, part } => {
                if let Some(transfer) = self.transfers.get(transfer_id) {
                    if let RldpTransfer::Outgoing(state) = transfer.value() {
                        if state.part().send_if_modified(|current_part| {
                            let same_part = *current_part == part;
                            *current_part += same_part as u32;
                            same_part
                        }) {
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
    async fn create_answer_handler(
        &self,
        adnl: &Arc<adnl::Node>,
        local_id: &adnl::NodeIdShort,
        peer_id: &adnl::NodeIdShort,
        transfer_id: TransferId,
    ) -> Result<Option<MessagePartsTx>> {
        use dashmap::mapref::entry::Entry;

        let (parts_tx, parts_rx) = match self.transfers.entry(transfer_id) {
            // Create new transfer
            Entry::Vacant(entry) => {
                let (parts_tx, parts_rx) = mpsc::unbounded_channel();
                entry.insert(RldpTransfer::Incoming(parts_tx.clone()));
                (parts_tx, parts_rx)
            }
            // Or do nothing if it already exists
            Entry::Occupied(_) => return Ok(None),
        };

        // Prepare context
        let mut incoming_context = IncomingContext {
            adnl: adnl.clone(),
            local_id: *local_id,
            peer_id: *peer_id,
            transfer: IncomingTransfer::new(transfer_id, self.max_answer_size),
            transfer_id,
        };

        // Spawn processing task
        let subscribers = self.subscribers.clone();
        let transfers = self.transfers.clone();
        let query_options = self.query_options;
        let force_compression = self.force_compression;
        tokio::spawn(async move {
            // Wait until incoming query is received
            incoming_context.receive(None, parts_rx).await;
            transfers.insert(transfer_id, RldpTransfer::Done);

            static QUERY: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

            let idx = QUERY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(query = idx, "received query");

            // Process query
            let outgoing_transfer_id = incoming_context
                .answer(
                    transfers.clone(),
                    subscribers,
                    query_options,
                    force_compression,
                )
                .await
                .unwrap_or_default();

            tracing::info!(query = idx, "answer");

            // Clear transfers in background
            tokio::time::sleep(query_options.completion_interval()).await;

            tracing::info!(query = idx, "removing");
            if let Some(outgoing_transfer_id) = outgoing_transfer_id {
                transfers.remove(&outgoing_transfer_id);
            }
            transfers.remove(&transfer_id);
        });

        // Clear incoming transfer on timeout
        let transfers = self.transfers.clone();
        let interval = self.query_options.completion_interval();
        tokio::spawn(async move {
            tokio::time::sleep(interval).await;
            transfers.insert(transfer_id, RldpTransfer::Done);
        });

        // Done
        Ok(Some(parts_tx))
    }
}

enum RldpTransfer {
    Incoming(MessagePartsTx),
    Outgoing(Arc<OutgoingTransferState>),
    Done,
}

struct IncomingContext {
    adnl: Arc<adnl::Node>,
    local_id: adnl::NodeIdShort,
    peer_id: adnl::NodeIdShort,
    transfer: IncomingTransfer,
    transfer_id: TransferId,
}

impl IncomingContext {
    async fn receive(
        &mut self,
        mut outgoing_transfer_state: Option<Arc<OutgoingTransferState>>,
        mut rx: MessagePartsRx,
    ) {
        // For each incoming message part
        while let Some(message) = rx.recv().await {
            // tracing::info!(
            //     part = message.part,
            //     total_size = message.total_size,
            //     seqno = message.seqno,
            //     data_len = message.data.len(),
            //     "received message"
            // );

            // Trying to process its data
            match self.transfer.process_chunk(message) {
                // If some data was successfully processed
                Ok(Some(reply)) => {
                    // Send `complete` or `confirm` message as reply
                    if let Err(e) =
                        self.adnl
                            .send_custom_message(&self.local_id, &self.peer_id, reply)
                    {
                        tracing::warn!("RLDP query error: {e}");
                    }
                }
                Err(e) => tracing::warn!("RLDP error: {e}"),
                _ => {}
            }

            // Notify state, that some reply was received
            if let Some(outgoing_transfer_state) = outgoing_transfer_state.take() {
                outgoing_transfer_state.set_reply();
            }

            // Increase `updates` counter
            self.transfer.state().send(()).ok();

            // Exit loop if all bytes were received
            if self.transfer.is_complete() {
                break;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn answer(
        mut self,
        transfers: Arc<FastDashMap<TransferId, RldpTransfer>>,
        subscribers: Arc<Vec<Arc<dyn QuerySubscriber>>>,
        query_options: QueryOptions,
        force_compression: bool,
    ) -> Result<Option<TransferId>> {
        // Deserialize incoming query
        let query = match OwnedRldpMessageQuery::from_data(self.transfer.take_data()) {
            Some(query) => query,
            None => return Err(TransfersCacheError::UnexpectedMessage.into()),
        };

        // Process query
        let ctx = SubscriberContext {
            adnl: &self.adnl,
            local_id: &self.local_id,
            peer_id: &self.peer_id,
        };
        let answer = match process_rldp_query(ctx, &subscribers, query, force_compression).await? {
            QueryProcessingResult::Processed(Some(answer)) => answer,
            QueryProcessingResult::Processed(None) => return Ok(None),
            QueryProcessingResult::Rejected => {
                return Err(TransfersCacheError::NoSubscribers.into())
            }
        };

        // Create outgoing transfer
        let outgoing_transfer_id = negate_id(self.transfer_id);
        let outgoing_transfer = OutgoingTransfer::new(answer, Some(outgoing_transfer_id));
        transfers.insert(
            outgoing_transfer_id,
            RldpTransfer::Outgoing(outgoing_transfer.state().clone()),
        );

        // Prepare context
        let outgoing_context = OutgoingContext {
            adnl: self.adnl.clone(),
            local_id: self.local_id,
            peer_id: self.peer_id,
            transfer: outgoing_transfer,
        };

        // Send answer
        outgoing_context.send(query_options, None).await?;

        // Done
        Ok(Some(outgoing_transfer_id))
    }
}

struct OutgoingContext {
    adnl: Arc<adnl::Node>,
    local_id: adnl::NodeIdShort,
    peer_id: adnl::NodeIdShort,
    transfer: OutgoingTransfer,
}

impl OutgoingContext {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn send(
        mut self,
        query_options: QueryOptions,
        roundtrip: Option<u64>,
    ) -> Result<(bool, u64)> {
        // Prepare timeout
        let mut timeout = query_options.compute_timeout(roundtrip);
        let mut roundtrip = roundtrip.unwrap_or_default();

        let waves_interval = Duration::from_millis(query_options.query_wave_interval_ms);

        let mut completed_part = self.transfer.state().part().subscribe();

        // For each outgoing message part
        while let Some(packet_count) = ok!(self.transfer.start_next_part()) {
            let wave_len = std::cmp::min(packet_count, query_options.query_wave_len);

            let part = *self.transfer.state().part().borrow();

            let mut start = Instant::now();

            let mut incoming_seqno = 0;
            'part: loop {
                // Send parts in waves
                for _ in 0..wave_len {
                    ok!(self.adnl.send_custom_message(
                        &self.local_id,
                        &self.peer_id,
                        ok!(self.transfer.prepare_chunk()),
                    ));
                }

                if tokio::time::timeout(waves_interval, completed_part.changed())
                    .await
                    .is_ok()
                    && self.transfer.is_finished_or_next_part(part)?
                {
                    break 'part;
                }

                // Update timeout on incoming packets
                let new_incoming_seqno = self.transfer.state().seqno_in();
                //tracing::info!(new_incoming_seqno, incoming_seqno);
                if new_incoming_seqno > incoming_seqno {
                    timeout = query_options.update_roundtrip(&mut roundtrip, &start);
                    incoming_seqno = new_incoming_seqno;
                    start = Instant::now();
                } else if is_timed_out(&start, timeout, incoming_seqno) {
                    return Ok((false, query_options.big_roundtrip(roundtrip)));
                }
            }

            // Update timeout
            timeout = query_options.update_roundtrip(&mut roundtrip, &start);
        }

        // Done
        Ok((true, roundtrip))
    }
}

#[derive(Copy, Clone)]
struct QueryOptions {
    query_wave_len: u32,
    query_wave_interval_ms: u64,
    query_min_timeout_ms: u64,
    query_max_timeout_ms: u64,
}

impl QueryOptions {
    /// Updates provided roundtrip and returns timeout
    fn update_roundtrip(&self, roundtrip: &mut u64, time: &Instant) -> u64 {
        *roundtrip = if *roundtrip == 0 {
            time.elapsed().as_millis() as u64
        } else {
            (*roundtrip + time.elapsed().as_millis() as u64) / 2
        };
        self.compute_timeout(Some(*roundtrip))
    }

    /// Clamps roundtrip to get valid timeout
    fn compute_timeout(&self, roundtrip: Option<u64>) -> u64 {
        match roundtrip {
            Some(roundtrip) if roundtrip > self.query_max_timeout_ms => self.query_max_timeout_ms,
            Some(roundtrip) => std::cmp::max(roundtrip, self.query_min_timeout_ms),
            None => self.query_max_timeout_ms,
        }
    }

    /// Computes roundtrip for invalid query
    fn big_roundtrip(&self, roundtrip: u64) -> u64 {
        std::cmp::min(roundtrip * 2, self.query_max_timeout_ms)
    }

    fn completion_interval(&self) -> Duration {
        Duration::from_millis(self.query_max_timeout_ms * 2)
    }
}

async fn process_rldp_query(
    ctx: SubscriberContext<'_>,
    subscribers: &[Arc<dyn QuerySubscriber>],
    mut query: OwnedRldpMessageQuery,
    force_compression: bool,
) -> Result<QueryProcessingResult<Vec<u8>>> {
    let answer_compression = match compression::decompress(&query.data) {
        Some(decompressed) => {
            query.data = decompressed;
            true
        }
        None => force_compression,
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
                    return Err(TransfersCacheError::AnswerSizeExceeded.into());
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

fn is_timed_out(time: &Instant, timeout: u64, updates: u32) -> bool {
    time.elapsed().as_millis() as u64 > timeout + timeout * (updates as u64) / 100
}

fn negate_id(id: [u8; 32]) -> [u8; 32] {
    id.map(|item| item ^ 0xff)
}

type MessagePartsTx = mpsc::UnboundedSender<MessagePart>;
type MessagePartsRx = mpsc::UnboundedReceiver<MessagePart>;

pub type TransferId = [u8; 32];

#[derive(thiserror::Error, Debug)]
enum TransfersCacheError {
    #[error("Unexpected message")]
    UnexpectedMessage,
    #[error("No subscribers for query")]
    NoSubscribers,
    #[error("Answer size exceeded")]
    AnswerSizeExceeded,
}
