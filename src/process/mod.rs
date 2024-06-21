use crate::request::{RequestT, StatusT};

pub(crate) trait Process {
    type R: RequestT;

    // DD: Is it too overengineered?
    fn from(&self) -> &<<Self::R as RequestT>::Status as StatusT>::Query;
    fn to(&self) -> &<<Self::R as RequestT>::Status as StatusT>::Query;

    #[must_use]
    async fn process(&self, updated: Self::R) -> Self::R;
}
