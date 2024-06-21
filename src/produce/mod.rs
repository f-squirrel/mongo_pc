mod producer;

pub(crate) use crate::request::RequestT;

pub(crate) trait Produce {
    type Request: RequestT;
    async fn produce(&self, data: impl Into<Self::Request>);
}
