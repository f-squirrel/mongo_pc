use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;

use super::filter::Filter;
use super::Watch;
use crate::handle::Handle;
use crate::request::RequestT;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::{bson::Document, Collection};
use tracing::{span, Level};

const ORDER_TRACKING_PERIOD: chrono::Duration = chrono::Duration::seconds(60);

pub struct Jitter<R: RequestT> {
    processed_data_id: Mutex<RefCell<HashSet<ObjectId>>>,
    processed_data_time: Mutex<RefCell<BTreeSet<DateTime<Utc>>>>,
    _phantom: std::marker::PhantomData<R>,
}

pub enum JitterError {
    OutOfOrder,
    Duplicate,
}

impl<R> Default for Jitter<R>
where
    R: RequestT,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<R> Jitter<R>
where
    R: RequestT,
{
    pub fn new() -> Self {
        Self {
            processed_data_id: Mutex::new(RefCell::new(HashSet::new())),
            processed_data_time: Mutex::new(RefCell::new(BTreeSet::new())),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn on_request(&self, request: &R, is_prewatched: bool) -> Result<(), JitterError> {
        if let Some(accepted_at) = self.processed_data_time.lock().unwrap().borrow().last() {
            if request.accepted_at() < accepted_at {
                return Err(JitterError::OutOfOrder);
            }
        }

        self.processed_data_time
            .lock()
            .unwrap()
            .borrow_mut()
            .retain(|&x| x > Utc::now() - ORDER_TRACKING_PERIOD);
        self.processed_data_time
            .lock()
            .unwrap()
            .borrow_mut()
            .insert(request.accepted_at().to_owned());

        if is_prewatched {
            self.processed_data_id
                .lock()
                .unwrap()
                .borrow_mut()
                .insert(request.oid().to_owned());
        } else if self
            .processed_data_id
            .lock()
            .unwrap()
            .borrow()
            .contains(request.oid())
        {
            return Err(JitterError::Duplicate);
        } else if !self.processed_data_id.lock().unwrap().borrow().is_empty() {
            tracing::info!("Clear hash cache, no duplicates");
            self.processed_data_id.lock().unwrap().borrow_mut().clear();
        }

        Ok(())
    }
}

pub struct Watcher<P: Handle<R>, R: RequestT> {
    collection: Collection<R>,
    watch_pipeline: Vec<Document>,
    pre_watch_filter: Document,
    handler: P,
    jitter: Jitter<R>,
}

#[async_trait::async_trait]
impl<H, R> Watch for Watcher<H, R>
where
    H: Handle<R>,
    R: RequestT,
{
    async fn watch(&self) {
        self.consume().await;
    }
}

impl<H, R> Watcher<H, R>
where
    H: Handle<R>,
    R: RequestT,
{
    pub fn new(collection: Collection<R>, filter: Filter, handler: H) -> Self {
        Self {
            collection,
            watch_pipeline: filter.watch_pipeline,
            pre_watch_filter: filter.pre_watch_filter.unwrap(),
            handler,
            jitter: Jitter::new(),
        }
    }

    async fn consume(&self) {
        tracing::debug!("Initiating change stream");
        let full_doc = Some(FullDocumentType::UpdateLookup);
        let opts = ChangeStreamOptions::builder()
            .full_document(full_doc)
            .build();

        let mut update_change_stream = self
            .collection
            .watch(self.watch_pipeline.clone(), opts)
            .await
            .expect("Failed to create change stream");

        tracing::debug!(
            "Initiating pre-watched data, filter: {:?}",
            self.pre_watch_filter
        );
        let mut pre_watched_data = self
            .collection
            .find(self.pre_watch_filter.clone(), None)
            .await
            .map_err(|e| {
                tracing::error!("Failed to look for pre-watched data: {:?}", e);
                e
            })
            .unwrap();

        let mut i = 0;
        while let Some(doc) = pre_watched_data
            .next()
            .await
            .transpose()
            .map_err(|e| {
                tracing::error!("Failed to receive document: {:?}", e);
                e
            })
            .unwrap()
        {
            let span = span!(Level::INFO, "consume", cid = doc.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Pre-watched data: {:?}", doc);

            match self.jitter.on_request(&doc, true) {
                Ok(_) => {
                    self.handler.handle(doc).await;
                    i += 1;
                }
                Err(JitterError::OutOfOrder) => {
                    tracing::warn!("Out of order data: {:?}", doc);
                }
                Err(JitterError::Duplicate) => {
                    tracing::info!(
                        "Ignored duplicate data, oid: {:?}, cid: {:?}",
                        doc.oid(),
                        doc.cid()
                    );
                    continue;
                }
            }
        }

        tracing::info!("Pre-watched data updated: {:?}", i);

        tracing::info!("Watching for updates");

        while let Some(event) = update_change_stream
            .next()
            .await
            .transpose()
            .map_err(|e| {
                tracing::error!("Failed to receive change event: {:?}", e);
                e
            })
            .unwrap()
        {
            tracing::debug!(
                "Update performed: {:?}, full document: {:?}",
                event.update_description,
                event.full_document
            );

            let updated = event.full_document.expect("Failed to get full document");

            let span = span!(Level::INFO, "consume", cid = updated.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Update performed: {:?}", event.update_description);

            match self.jitter.on_request(&updated, false) {
                Ok(_) => {
                    self.handler.handle(updated).await;
                    i += 1;
                }
                Err(JitterError::OutOfOrder) => {
                    tracing::warn!("Out of order data: {:?}", updated);
                }
                Err(JitterError::Duplicate) => {
                    tracing::info!(
                        "Ignored duplicate data, oid: {:?}, cid: {:?}",
                        updated.oid(),
                        updated.cid()
                    );
                    continue;
                }
            }
        }
        tracing::info!("Change stream was closed, consumed {i}, exiting");
    }
}
