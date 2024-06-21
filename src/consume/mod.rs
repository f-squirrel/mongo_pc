pub(crate) mod consumer;

pub(crate) trait Consume {
    async fn consume(&self);
}

pub(crate) use consumer::Consumer;
