use crate::request::RequestT;

pub(crate) mod handler;

#[async_trait::async_trait]
pub(crate) trait Handle<R: RequestT>: Send + Sync{
    async fn handle(&self, updated: R, is_prewatched: bool);
}
