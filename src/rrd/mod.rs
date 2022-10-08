use async_trait::async_trait;
use core::time;
use sqlx::{self, ConnectOptions, Sqlite, SqliteConnection};
use std::{io::Write, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    SQLXError(#[from] sqlx::Error),

    #[error("{0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("RRD error: {error}")]
    RRDError { error: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[async_trait]
pub trait Slot {
    async fn counter(&mut self, key: &str, value: f64) -> Result<()>;
    async fn key(&mut self) -> Result<i64>;
}

#[async_trait]
pub trait RRD<S>
where
    S: Slot,
{
    async fn slot(self) -> Result<S>;
    async fn counters(&mut self, since: std::time::SystemTime) -> Result<Vec<(String, i64)>>;
    async fn last(self, key: &str) -> Result<Option<f64>>;
}

struct RRDImpl {
    pub connection: SqliteConnection,
    pub window: i64,
    pub retention: i64,
}

struct RRDSlotImpl {
    pub connection: SqliteConnection,
    pub key: i64,
}

#[async_trait]
impl Slot for RRDSlotImpl {
    async fn counter(&mut self, key: &str, value: f64) -> Result<()> {
        RRDSlotImpl::set_last(self, key, &value).await?;
        let last = match RRDSlotImpl::get_last(self, key).await? {
            None => return Ok(()),
            Some(t) => t,
        };
        let mut diff = 0.0;
        if value >= last {
            diff = value - last;
        } else {
            // this is either an overflow
            // or counter has been reset (node was restarted hence)
            // metrics are counting from 0 again.
            // hence it's safer to assume diff is just the value
            // reported
            diff = value;
        }
        sqlx::query("REPLACE INTO usage (timestamp, metric, value) VALUES (?, ?, ?);")
            .bind(self.key as f64)
            .bind(key)
            .bind(value)
            .execute(&mut self.connection)
            .await?;

        Ok(())
    }

    async fn key(&mut self) -> Result<i64> {
        Ok(self.key)
    }
}

#[async_trait]
impl RRD<RRDSlotImpl> for RRDImpl {
    async fn last(self, metric: &str) -> Result<Option<f64>> {
        let mut slot = RRDImpl::slot(self).await?;
        Ok(slot.get_last(metric).await?)
    }

    async fn counters(&mut self, since: std::time::SystemTime) -> Result<Vec<(String, i64)>> {
        let ts = since.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
        let ts = (ts / self.window) * self.window;

        let records: Vec<(String, i64)> = sqlx::query_as(
            "SELECT metric, SUM(value) FROM usage 
            GROUP BY metric 
            HAVING timestamp >= ?",
        )
        .bind(ts)
        .fetch_all(&mut self.connection)
        .await?;

        Ok(records)
    }

    async fn slot(mut self) -> Result<RRDSlotImpl> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let ts = (now / self.window) * self.window;
        RRDImpl::retain(&mut self, ts).await?;
        Ok(RRDSlotImpl {
            connection: self.connection,
            key: ts,
        })
    }
}

impl RRDImpl {
    pub async fn new<P: AsRef<Path>>(
        path: P,
        window: time::Duration,
        retention: time::Duration,
    ) -> Result<RRDImpl> {
        if window.is_zero() {
            return Err(Error::RRDError {
                error: String::from("invalide window, can't be zero"),
            });
        }
        if retention.is_zero() {
            return Err(Error::RRDError {
                error: String::from("invalid retention, can't be zero"),
            });
        }
        if retention < window {
            return Err(Error::RRDError {
                error: String::from("retention can't be smaller than window"),
            });
        }

        let mut connection = sqlx::sqlite::SqliteConnectOptions::new()
            .create_if_missing(true)
            .filename(path)
            .connect()
            .await?;

        sqlx::query::<Sqlite>(
            "CREATE TABLE IF NOT EXISTS usage (
                timestamp INTEGER NOT NULL, 
                metric TEXT NOT NULL, 
                value FLOAT NOT NULL,
                PRIMARY KEY (timestamp, metric)
                )",
        )
        .execute(&mut connection)
        .await?;

        sqlx::query::<Sqlite>(
            "CREATE TABLE IF NOT EXISTS last (
                timestamp INTEGER NOT NULL, 
                metric TEXT NOT NULL UNIQUE, 
                value FLOAT NOT NULL,
                PRIMARY KEY (timestamp, metric)
                )",
        )
        .execute(&mut connection)
        .await?;

        sqlx::query::<Sqlite>("CREATE INDEX IF NOT EXISTS ts_index ON usage (timestamp)")
            .execute(&mut connection)
            .await?;

        sqlx::query::<sqlx::Sqlite>("CREATE INDEX IF NOT EXISTS ts_index ON last (timestamp)")
            .execute(&mut connection)
            .await?;

        Ok(RRDImpl {
            connection,
            retention: retention.as_secs() as i64,
            window: window.as_secs() as i64,
        })
    }

    pub async fn print<W: Write>(&mut self, mut writer: W) -> Result<W> {
        RRDImpl::print_last_usage(self, &mut writer).await?;

        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage;")
            .fetch_all(&mut self.connection)
            .await?;
        for ts in timestamps {
            RRDImpl::print_ts(self, ts, &mut writer).await?;
        }
        Ok(writer)
    }

    async fn print_last_usage<W: Write>(&mut self, mut writer: W) -> Result<()> {
        writer.write_fmt(format_args!(".last\n"))?;
        let records: Vec<(String, f64)> = sqlx::query_as("SELECT metric, usage FROM last")
            .fetch_all(&mut self.connection)
            .await?;
        for (metric, usage) in records {
            writer.write_fmt(format_args!("\t{}: {}\n", metric, usage))?
        }
        Ok(())
    }

    async fn print_ts<W: Write>(&mut self, ts: i64, mut writer: W) -> Result<()> {
        let records: Vec<(String, f64)> =
            sqlx::query_as("SELECT metric, usage FROM usage WHERE timestamp = ?")
                .bind(ts)
                .fetch_all(&mut self.connection)
                .await?;
        for (metric, usage) in records {
            writer.write_fmt(format_args!("\t{}: {}\n", metric, usage))?
        }
        Ok(())
    }

    async fn retain(&mut self, now: i64) -> Result<()> {
        // should retain be unsigned?
        let retain = (now - self.retention) as i64;
        sqlx::query("DELETE FROM usage WHERE timestamp <= ?")
            .bind(retain)
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }

    pub async fn slots(&mut self) -> Result<Vec<i64>> {
        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage")
            .fetch_all(&mut self.connection)
            .await?;
        Ok(timestamps)
    }
}

impl RRDSlotImpl {
    async fn get_last(&mut self, key: &str) -> Result<Option<f64>> {
        let last: Vec<f64> =
            sqlx::query_scalar("SELECT value FROM last WHERE timestamp = ? AND metric = ?")
                .bind(self.key as f64)
                .bind(key)
                .fetch_all(&mut self.connection)
                .await?;
        if last.is_empty() {
            return Ok(None);
        }
        Ok(Some(last[0]))
    }

    async fn set_last(&mut self, key: &str, value: &f64) -> Result<()> {
        sqlx::query("REPLACE INTO last (timestamp, metric, value) VALUES (?, ?, ?);")
            .bind(self.key as f64)
            .bind(key)
            .bind(value)
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }
}
