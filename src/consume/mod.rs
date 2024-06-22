pub(crate) mod consumer;
pub(crate) mod filter;

pub(crate) trait Consume {
    async fn consume(&self);
}

pub(crate) use consumer::Consumer;
pub(crate) use filter::FilterBuilder;
