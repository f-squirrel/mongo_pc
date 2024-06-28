pub(crate) mod filter;
pub(crate) mod watcher;

#[async_trait::async_trait]
pub(crate) trait Watch: Sync + Send {
    async fn watch(&self);
}

pub(crate) use watcher::Watcher;
