use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;

use super::Handle;
use crate::process::Process;
use crate::request::{RequestT, StatusT};

use chrono::{DateTime, Utc};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{self, doc};
use mongodb::Collection;

const ORDER_TRACKING_PERIOD: chrono::Duration = chrono::Duration::seconds(60);

#[allow(dead_code)]
enum Processor {
    Reader,
    Writer,
}

pub(crate) struct Handler<P: Process, R: RequestT> {
    collection: Collection<R>,
    handler: P,
    // processed_data_id: Mutex<RefCell<HashSet<ObjectId>>>,
    // processed_data_time: Mutex<RefCell<BTreeSet<DateTime<Utc>>>>,
    _phantom: std::marker::PhantomData<R>,
}

#[async_trait::async_trait]
impl<H, R> Handle<R> for Handler<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    async fn handle(&self, updated: R, is_prewatched: bool) {
        // if let Some(accepted_at) = self.processed_data_time.get_mut().unwrap().last() {
        //     if updated.accepted_at() < accepted_at {
        //         tracing::warn!("Out of order data: {:?}", updated)
        //     }
        // }

        // self.processed_data_time
        //     .borrow_mut()
        //     .retain(|&x| x > Utc::now() - ORDER_TRACKING_PERIOD);
        // self.processed_data_time
        //     .borrow_mut()
        //     .insert(updated.accepted_at().to_owned());

        // if is_prewatched {
        //     self.processed_data_id
        //         .borrow_mut()
        //         .insert(updated.oid().to_owned());
        // } else if self.processed_data_id.borrow_mut().contains(updated.oid()) {
        //     tracing::info!(
        //         "Ignored duplicate data, oid: {:?}, cid: {:?}",
        //         updated.oid(),
        //         updated.cid()
        //     );
        //     return;
        // } else if !self.processed_data_id.borrow_mut().is_empty() {
        //     tracing::info!("Clear hash cache, no duplicates");
        //     self.processed_data_id.borrow_mut().clear();
        // }

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
}
