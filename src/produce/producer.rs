use mongodb::Collection;
use tracing::{span, Level};

use super::Produce;
use crate::request::RequestT;

pub(crate) struct Producer<R> {
    collection: Collection<R>,
}

impl<R> Producer<R> {
    pub(crate) fn new(collection: Collection<R>) -> Self {
        Self { collection }
    }
}

#[async_trait::async_trait]
impl<R> Produce for Producer<R>
where
    R: RequestT,
{
    type Request = R;

    // async fn produce(&self, data: impl Into<Self::Request> + Send) {
    async fn produce(&self, data: Self::Request) {
        // let data = data.into();
        let span = span!(Level::INFO, "request", cid = data.cid().to_string());
        let _enter = span.enter();

        tracing::info!("Received: {:?}", data);
        self.collection
            .insert_one(&data, None)
            .await
            .map_err(|e| {
                tracing::error!("Failed to produce document: {:?}", e);
                e
            })
            .unwrap();
        tracing::trace!("Produced: {:?}", data);
    }
}
