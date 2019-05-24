use futures::Future;
use redis::r#async::ConnectionLike;
use redis::{IntoConnectionInfo, RedisError, RedisResult};
use tokio_resource_pool::{CheckOut, Manage, Pool};
pub use tokio_resource_pool::CheckOutFuture;

/// Manages the lifecycle of connections to a single Redis server.
pub struct RedisManager {
    client: redis::Client,
}

impl RedisManager {
    /// Creates a new `RedisManager` from anything that can be converted to a `ConnectionInfo`.
    ///
    /// # Example
    ///
    /// ```
    /// # use tokio_redis_pool::RedisManager;
    /// # use redis::RedisResult;
    /// # fn main() -> RedisResult<()> {
    /// let manager = RedisManager::new("redis://127.0.0.1:6379")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(params: impl IntoConnectionInfo) -> RedisResult<Self> {
        let client = redis::Client::open(params)?;
        Ok(Self { client })
    }
}

impl Manage for RedisManager {
    type Resource = redis::r#async::Connection;

    type CheckOut = RedisCheckOut;

    type Error = RedisError;

    type Future = Box<dyn Future<Item = Self::Resource, Error = Self::Error> + Send>;

    fn create(&self) -> Self::Future {
        Box::new(self.client.get_async_connection())
    }
}

/// A resource `Pool` specialized for Redis connections.
pub type RedisPool = Pool<RedisManager>;

/// A check out of a Redis connection from the pool.
///
/// It implements `ConnectionLike`, so you can pass it directly to functions such as
/// `Pipeline::query`.
///
/// # Example
///
/// ```
/// # use futures::future::Future;
/// # use redis::RedisResult;
/// # use tokio_redis_pool::{RedisManager, RedisPool};
/// # let manager = RedisManager::new("redis://127.0.0.1:6379").unwrap();
/// # let (pool, _) = RedisPool::new(4, manager);
/// tokio::run(
///     pool
///         .check_out()
///         .and_then(|connection| redis::cmd("INFO").query_async::<_, redis::InfoDict>(connection))
///         .map(|(_checked_out_connection, info)| println!("{:#?}", info))
///         .map_err(|error| eprintln!("{}", error)),
/// );
/// ```
pub struct RedisCheckOut {
    inner: CheckOut<RedisManager>,
}

impl ConnectionLike for RedisCheckOut {
    fn req_packed_command(
        self,
        cmd: Vec<u8>,
    ) -> Box<dyn Future<Item = (Self, redis::Value), Error = RedisError> + Send> {
        Box::new(
            self.inner
                .lend(move |connection| connection.req_packed_command(cmd)),
        )
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> Box<dyn Future<Item = (Self, Vec<redis::Value>), Error = RedisError> + Send> {
        Box::new(
            self.inner
                .lend(move |connection| connection.req_packed_commands(cmd, offset, count)),
        )
    }

    fn get_db(&self) -> i64 {
        self.inner.get_db()
    }
}

impl From<CheckOut<RedisManager>> for RedisCheckOut {
    fn from(inner: CheckOut<RedisManager>) -> Self {
        Self { inner }
    }
}
