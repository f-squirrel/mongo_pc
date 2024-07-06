use crate::request::RequestT;

pub mod handler;

#[async_trait::async_trait]
pub trait Handle<R: RequestT>: Send + Sync {
    async fn handle(&self, updated: R);
}
