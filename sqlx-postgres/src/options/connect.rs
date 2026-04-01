use crate::connection::ConnectOptions;
use crate::error::Error;
use crate::net::Socket;
use crate::{PgConnectOptions, PgConnection};
use futures_core::future::BoxFuture;
use log::LevelFilter;
use sqlx_core::Url;
use std::time::Duration;

impl PgConnectOptions {
    /// Establish a connection over a pre-connected socket.
    ///
    /// This performs the PostgreSQL startup handshake, TLS upgrade
    /// (if configured), and authentication over the provided socket.
    ///
    /// The socket must already be connected to a PostgreSQL-compatible server.
    /// This enables custom transports such as in-memory pipes, simulation
    /// frameworks (e.g. turmoil), SSH tunnels, or SOCKS proxies.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use sqlx::postgres::PgConnectOptions;
    ///
    /// let options = PgConnectOptions::new()
    ///     .username("postgres")
    ///     .database("mydb");
    ///
    /// let stream = tokio::net::TcpStream::connect("127.0.0.1:5432").await?;
    /// let conn = options.connect_with_socket(stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_socket<S: Socket>(&self, socket: S) -> Result<PgConnection, Error> {
        PgConnection::connect_with_socket(self, socket).await
    }
}

impl ConnectOptions for PgConnectOptions {
    type Connection = PgConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::parse_from_url(url)
    }

    fn to_url_lossy(&self) -> Url {
        self.build_url()
    }

    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Error>>
    where
        Self::Connection: Sized,
    {
        Box::pin(PgConnection::establish(self))
    }

    fn log_statements(mut self, level: LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}
