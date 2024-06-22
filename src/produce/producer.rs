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

impl<R> Produce for Producer<R>
where
    R: RequestT,
{
    type Request = R;

    async fn produce(&self, data: impl Into<Self::Request>) {
        let data = data.into();
        let span = span!(Level::INFO, "request", cid = data.cid().to_string());
        let _enter = span.enter();

        tracing::trace!("Received: {:?}", data);
        self.collection.insert_one(&data, None).await.unwrap();
        tracing::trace!("Produced: {:?}", data);
    }
}
