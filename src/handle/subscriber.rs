use super::Handle;
use crate::process::Process;
use crate::request::{RequestT, StatusT};

pub(crate) struct Subscriber<P: Process, R: RequestT> {
    handler: P,
    _phantom: std::marker::PhantomData<R>,
}

impl<H, R> Subscriber<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<H, R> Handle<R> for Subscriber<H, R>
where
    H: Process<R = R>,
    R: RequestT,
{
    async fn handle(&self, updated: R) {
        assert_eq!(
            std::mem::discriminant(self.handler.from()),
            std::mem::discriminant(&updated.status().to_query()),
            "Received status: {:?}, expected: {:?}",
            updated.status().to_query(),
            self.handler.from(),
        );

        let _ = self.handler.process(updated).await;
    }
}
