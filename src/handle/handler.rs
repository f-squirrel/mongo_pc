use super::Handle;
use crate::process::Process;
use crate::request::{RequestT, StatusT};

use mongodb::bson::{self, doc};
use mongodb::Collection;

pub(crate) struct Handler<P: Process, R: RequestT> {
    collection: Collection<R>,
    handler: P,
    expected_output_statuses:
        Vec<std::mem::Discriminant<<<R as RequestT>::Status as StatusT>::Query>>,
}

impl<H, R> Handler<H, R>
where
    H: Process<R = R>, /*  + Handle<R> */
    R: RequestT,
{
    pub(crate) fn new(collection: Collection<R>, handler: H) -> Self {
        let expected_output_statuses: Vec<_> =
            handler.to().iter().map(std::mem::discriminant).collect();

        Self {
            collection,
            handler,
            expected_output_statuses,
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

        let to_status = std::mem::discriminant(&updated.status().to_query());
        assert!(
            self.expected_output_statuses.contains(&to_status),
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
