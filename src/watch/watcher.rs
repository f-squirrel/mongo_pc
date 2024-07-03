use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;

use super::filter::Filter;
use super::Watch;
use crate::handle::Handle;
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

// pub(crate) struct Watcher<P: Process, R: RequestT> {
pub(crate) struct Watcher<P: Handle<R>, R: RequestT> {
    collection: Collection<R>,
    watch_pipeline: Vec<Document>,
    pre_watch_filter: Document,
    handler: P,
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
    pub(crate) fn new(collection: Collection<R>, filter: Filter, handler: H) -> Self {
        Self {
            collection,
            watch_pipeline: filter.watch_pipeline,
            pre_watch_filter: filter.pre_watch_filter.unwrap(),
            handler,
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
            self.handler.handle(doc, true).await;
            i += 1;
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
            self.handler.handle(updated, false).await;
            i += 1;
        }
        tracing::info!("Change stream was closed, consumed {i}, exiting");
    }
}
