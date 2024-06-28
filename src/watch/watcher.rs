use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};

use super::filter::Filter;
use super::Watch;
use crate::process::Process;
use crate::request::{RequestT, StatusQueryT, StatusT};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{self, doc};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::{bson::Document, Collection};
use tracing::{span, Level};

const ORDER_TRACKING_PERIOD: chrono::Duration = chrono::Duration::seconds(60);

pub(crate) struct Watcher<P: Process, R: RequestT> {
    collection: Collection<R>,
    watch_pipeline: Vec<Document>,
    pre_watch_filter: Document,
    handler: P,
    processed_data_id: RefCell<HashSet<ObjectId>>,
    processed_data_time: RefCell<BTreeSet<DateTime<Utc>>>,
}

impl<H, R> Watch for Watcher<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    async fn watch(&self) {
        self.consume().await;
    }
}

impl<H, R> Watcher<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    pub(crate) fn new(collection: Collection<R>, filter: Filter, handler: H) -> Self {
        let pre_watch_filter = if filter.pre_watch_filter.is_none() {
            let from_status =
                bson::ser::to_bson(handler.from()).expect("Failed to serialize 'from' status");
            doc! {handler.from().query_id(): from_status}
        } else {
            filter.pre_watch_filter.unwrap()
        };

        Self {
            collection,
            watch_pipeline: filter.watch_pipeline,
            pre_watch_filter,
            handler,
            processed_data_id: RefCell::new(HashSet::new()),
            processed_data_time: RefCell::new(BTreeSet::new()),
        }
    }

    async fn handle_update(&self, updated: R, is_prewatched: bool) {
        if let Some(accepted_at) = self.processed_data_time.borrow_mut().last() {
            if updated.accepted_at() < accepted_at {
                tracing::warn!("Out of order data: {:?}", updated)
            }
        }

        self.processed_data_time
            .borrow_mut()
            .retain(|&x| x > Utc::now() - ORDER_TRACKING_PERIOD);
        self.processed_data_time
            .borrow_mut()
            .insert(updated.accepted_at().to_owned());

        if is_prewatched {
            self.processed_data_id
                .borrow_mut()
                .insert(updated.oid().to_owned());
        } else if self.processed_data_id.borrow_mut().contains(updated.oid()) {
            tracing::info!(
                "Ignored duplicate data, oid: {:?}, cid: {:?}",
                updated.oid(),
                updated.cid()
            );
            return;
        } else if !self.processed_data_id.borrow_mut().is_empty() {
            tracing::info!("Clear hash cache, no duplicates");
            self.processed_data_id.borrow_mut().clear();
        }

        assert_eq!(
            std::mem::discriminant(self.handler.from()),
            std::mem::discriminant(&updated.status().to_query()),
            "Received status: {:?}, expected: {:?}",
            updated.status().to_query(),
            self.handler.from(),
        );

        let updated = self.handler.process(updated).await;

        assert_eq!(
            std::mem::discriminant(&updated.status().to_query()),
            std::mem::discriminant(self.handler.to()),
            "Updated to status: {:?}, expected: {:?}",
            updated.status().to_query(),
            self.handler.to(),
        );

        let updated_document =
            bson::to_document(&updated).expect("Failed to serialized processed document");
        let query = doc! { "_id" : updated.oid() };
        let updated_doc = doc! {
            "$set": updated_document
        };
        tracing::debug!("Document updated: {:?}", updated_doc);
        self.collection
            .update_one(query, updated_doc, None)
            .await
            .expect("Failed to update document");
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
            let span = span!(Level::INFO, "request", cid = doc.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Pre-watched data: {:?}", doc);
            self.handle_update(doc, true).await;
            i += 1;
        }

        tracing::info!("Pre-watched data updated: {:?}", i);

        tracing::info!("Watching for updates");

        let mut i = 0;

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

            let span = span!(Level::INFO, "request", cid = updated.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Update performed: {:?}", event.update_description);
            self.handle_update(updated, false).await;

            i += 1;
        }
        tracing::info!("Change stream was closed, consumed {i}, exiting");
    }
}
