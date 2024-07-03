use super::Handle;
use crate::process::Process;
use crate::request::{RequestT, StatusT};

use mongodb::bson::{self, doc};
use mongodb::Collection;

pub(crate) struct Handler<P: Process, R: RequestT> {
    collection: Collection<R>,
    handler: P,
}

impl<H, R> Handler<H, R>
where
    H: Process<R = R>, /*  + Handle<R> */
    R: RequestT,
{
    pub(crate) fn new(collection: Collection<R>, handler: H) -> Self {
        Self {
            collection,
            handler,
        }
    }
}

#[async_trait::async_trait]
impl<H, R> Handle<R> for Handler<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    async fn handle(&self, updated: R) {
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
