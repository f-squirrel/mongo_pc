mod consumer;

// use mod crate::api::cid::Cid;

/// The `Consume` trait defines asynchronous behavior for consuming updates.
///
/// Implementors of this trait are expected to listen for and process updates
/// once the `consume` method is called. The `consume` method is asynchronous,
/// allowing for non-blocking operation and compatibility with async runtime environments.
///
/// # Examples
///
/// Basic implementation of the `Consume` trait:
///
/// ```rust
/// struct MyConsumer;
///
/// impl Consume for MyConsumer {
///     async fn consume(&self) {
///         // Implementation for consuming updates
///     }
/// }
/// ```
pub(crate) trait Consume {
    async fn consume(&self);
}
