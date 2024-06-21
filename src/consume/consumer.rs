use std::collections::{BTreeSet, HashSet};

use crate::request::{RequestT, StatusT};
use crate::{api::cid::Cid, process::Process};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use mongodb::bson::{self, doc};
use mongodb::options::{ChangeStreamOptions, FullDocumentType};
use mongodb::{bson::Document, Collection};
use tracing::{span, Level};

use crate::UPDATES_NUM;

pub(crate) struct Consumer<H: Process, D: RequestT> {
    collection: Collection<D>,
    watch_pipeline: Vec<Document>,
    handler: H,
}

pub(crate) trait Consume {
    async fn consume(&self);
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
    pub(crate) fn new(
        collection: Collection<R>,
        watch_pipeline: Vec<Document>,
        handler: H,
    ) -> Self {
        Self {
            collection,
            watch_pipeline,
            handler,
        }
    }

    async fn handle_update(
        &self,
        updated: R,
        is_prewatched: bool,
        hash_set: &mut HashSet<Cid>,
        ord_time: &mut BTreeSet<DateTime<Utc>>,
    ) {
        if let Some(accepted_at) = ord_time.last() {
            if updated.accepted_at() < accepted_at {
                tracing::warn!("Out of order data: {:?}", updated)
            }
        }

        ord_time.insert(updated.accepted_at().to_owned());

        if is_prewatched {
            hash_set.insert(updated.cid().to_owned());
        } else if hash_set.contains(updated.cid()) {
            tracing::info!("Ignored duplicate data, cid: {:?}", updated.cid());
            return;
        } else if !hash_set.is_empty() {
            tracing::info!("Clear hash cache, no duplicates");
            hash_set.clear();
        }

        assert_eq!(
            std::mem::discriminant(&updated.status().to_query()),
            std::mem::discriminant(self.handler.from()),
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

        let from_status = bson::ser::to_bson(self.handler.from()).unwrap();
        let name = from_status;
        let filter = doc! {"status.tag": name};

        tracing::debug!("Initiating pre-watched data, filter: {filter:?}");
        let mut pre_watched_data = self.collection.find(filter, None).await.unwrap();

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