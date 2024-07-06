pub mod filter;
pub mod watcher;

#[async_trait::async_trait]
pub trait Watch: Sync + Send {
    async fn watch(&self);
}

pub use watcher::Watcher;
