mod producer;

pub(crate) use crate::request::RequestT;

#[async_trait::async_trait]
pub(crate) trait Produce: Send + Sync {
    type Request: RequestT;
    // async fn produce(&self, data: impl Into<Self::Request>);
    async fn produce(&self, data: Self::Request);
}

pub(crate) use producer::Producer;
