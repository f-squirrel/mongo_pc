use crate::request::{RequestT, StatusT};

#[async_trait::async_trait]
pub(crate) trait Process: Send + Sync {
    type R: RequestT;

    // DD: Is it too overengineered?
    fn from(&self) -> &<<Self::R as RequestT>::Status as StatusT>::Query;
    fn to(&self) -> &[<<Self::R as RequestT>::Status as StatusT>::Query];

    #[must_use]
    async fn process(&self, updated: Self::R) -> Self::R;
}
