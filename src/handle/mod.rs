use crate::request::RequestT;

pub(crate) mod handler;

pub(crate) trait Handle<R: RequestT> {
    async fn handle(&self, updated: R, is_prewatched: bool);
}
