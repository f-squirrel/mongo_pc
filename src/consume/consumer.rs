use std::collections::{BTreeSet, HashSet};

use super::filter::Filter;
use super::Consume;
use crate::process::Process;
use crate::request::{RequestT, StatusT};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{self, doc};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::{bson::Document, Collection};
use tracing::{span, Level};

use crate::UPDATES_NUM;

pub(crate) struct Consumer<P: Process, R: RequestT> {
    collection: Collection<R>,
    watch_pipeline: Vec<Document>,
    pre_watch_filter: Document,
    handler: P,
}

impl<H, R> Consume for Consumer<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    async fn consume(&self) {
        self.consume().await;
    }
}

impl<H, R> Consumer<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    pub(crate) fn new(collection: Collection<R>, filter: Filter, handler: H) -> Self {
        let pre_watch_filter = if filter.pre_watch_filter.is_none() {
            let from_status = bson::ser::to_bson(handler.from()).unwrap();
            doc! {"status.tag": from_status}
        } else {
            filter.pre_watch_filter.unwrap()
        };

        Self {
            collection,
            watch_pipeline: filter.watch_pipeline,
            pre_watch_filter,
            handler,
        }
    }

    async fn handle_update(
        &self,
        updated: R,
        is_prewatched: bool,
        hash_set: &mut HashSet<ObjectId>,
        ord_time: &mut BTreeSet<DateTime<Utc>>,
    ) {
        if let Some(accepted_at) = ord_time.last() {
            if updated.accepted_at() < accepted_at {
                tracing::warn!("Out of order data: {:?}", updated)
            }
        }

        ord_time.insert(updated.accepted_at().to_owned());

        if is_prewatched {
            hash_set.insert(updated.oid().to_owned());
        } else if hash_set.contains(updated.oid()) {
            tracing::info!(
                "Ignored duplicate data, oid: {:?}, cid: {:?}",
                updated.oid(),
                updated.cid()
            );
            return;
        } else if !hash_set.is_empty() {
            tracing::info!("Clear hash cache, no duplicates");
            hash_set.clear();
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

        let updated_document = bson::to_document(&updated).unwrap();
        let query = doc! { "_id" : updated.oid() };
        let updated_doc = doc! {
            "$set": updated_document
        };
        tracing::debug!("Document updated: {:?}", updated_doc);
        self.collection
            .update_one(query, updated_doc, None)
            .await
            .unwrap();
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
            .unwrap();

        tracing::debug!(
            "Initiating pre-watched data, filter: {:?}",
            self.pre_watch_filter
        );
        let mut pre_watched_data = self
            .collection
            .find(self.pre_watch_filter.clone(), None)
            .await
            .unwrap();

        let mut ord_time = BTreeSet::new();

        let mut hash_set = HashSet::new();
        let mut i = 0;
        while let Some(doc) = pre_watched_data.next().await.transpose().unwrap() {
            let span = span!(Level::INFO, "request", cid = doc.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Pre-watched data: {:?}", doc);
            self.handle_update(doc, true, &mut hash_set, &mut ord_time)
                .await;
            i += 1;
        }

        tracing::info!("Pre-watched data updated: {:?}", i);

        tracing::info!("Watching for updates");

        let mut i = 0;
        while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
            tracing::debug!(
                "Update performed: {:?}, full document: {:?}",
                event.update_description,
                event.full_document
            );

            let updated = event.full_document.unwrap();

            let span = span!(Level::INFO, "request", cid = updated.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Update performed: {:?}", event.update_description);
            self.handle_update(updated, false, &mut hash_set, &mut ord_time)
                .await;

            i += 1;
            if i >= UPDATES_NUM {
                tracing::info!("Processed all updates");
                break;
            }
        }
        tracing::info!("Consumed {i}, no more updates");
    }
}
