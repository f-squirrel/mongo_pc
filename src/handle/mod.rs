use crate::request::RequestT;

pub(crate) mod handler;
pub(crate) mod subscriber;

#[async_trait::async_trait]
pub(crate) trait Handle<R: RequestT>: Send + Sync {
    async fn handle(&self, updated: R);
}
