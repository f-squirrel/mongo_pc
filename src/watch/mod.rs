pub(crate) mod filter;
pub(crate) mod watcher;

pub(crate) trait Watch {
    async fn watch(&self);
}

pub(crate) use watcher::Watcher;
