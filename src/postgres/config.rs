use secrecy::{ExposeSecret, Secret};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PostgresStorageConfig {
    pub key_prefix: String,
    pub username: String,
    pub password: Secret<String>,
    pub host: String,
    pub port: u16,
    pub database_name: String,
    pub require_ssl: bool,

    pub min_connections: Option<u32>,

    pub max_connections: Option<u32>,

    pub max_lifetime: Option<Duration>,

    pub idle_timeout: Option<Duration>,
}

impl PostgresStorageConfig {
    #[allow(dead_code)]
    pub fn connection_string(&self) -> Secret<String> {
        let connection = format!(
            "postgres://{db_user}:{db_password}@{host}:{port}/{db_name}",
            db_user = self.username,
            db_password = self.password.expose_secret(),
            host = self.host,
            port = self.port,
            db_name = self.database_name,
        );

        Secret::new(connection)
    }

    #[allow(dead_code)]
    pub fn pg_pool_options(&self) -> PgPoolOptions {
        let mut options = PgPoolOptions::new()
            .max_lifetime(self.max_lifetime)
            .idle_timeout(self.idle_timeout);

        if let Some(min) = self.min_connections {
            options = options.min_connections(min);
        }

        if let Some(max) = self.max_connections {
            options = options.max_connections(max);
        }

        options
    }

    #[allow(dead_code)]
    pub fn pg_connect_options_without_db(&self) -> PgConnectOptions {
        let ssl_mode = if self.require_ssl {
            PgSslMode::Require
        } else {
            PgSslMode::Prefer
        };

        PgConnectOptions::new()
            .host(&self.host)
            .username(&self.username)
            .password(self.password.expose_secret())
            .port(self.port)
            .ssl_mode(ssl_mode)
    }

    #[allow(dead_code)]
    pub fn pg_connect_options_with_db(&self) -> PgConnectOptions {
        self.pg_connect_options_without_db()
            .database(&self.database_name)
    }
}

impl PartialEq for PostgresStorageConfig {
    fn eq(&self, other: &Self) -> bool {
        self.username == other.username
            && self.password.expose_secret() == other.password.expose_secret()
            && self.host == other.host
            && self.port == other.port
            && self.database_name == other.database_name
            && self.require_ssl == other.require_ssl
            && self.min_connections == other.min_connections
            && self.max_connections == other.max_connections
            && self.max_lifetime == other.max_lifetime
            && self.idle_timeout == other.idle_timeout
    }
}
