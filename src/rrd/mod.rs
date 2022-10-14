use anyhow::Result;
use async_trait::async_trait;
use core::time;
use sqlx::{self, Sqlite};
use std::{io::Write, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid window, can't be zero")]
    InvalidWindow,

    #[error("invalid retention, can't be zero or less than window")]
    InvalidRetention,
}

/// Slot provides the functionality to set or overwrite the value of any metric
/// at a specific timestamp.
#[async_trait]
pub trait Slot {
    /// Counter sets (or overrides) the current stored value for this key,
    /// with value
    async fn counter(&mut self, key: &str, value: f64) -> Result<()>;
    /// Key return the key of the slot which is the window timestamp
    async fn key(&self) -> Result<i64>;
}

/// RRD is a round robin database of fixed size which is specified on creation
/// RRD stores `counter` values. Which means values that can only go UP.
/// then it's easy to compute the increase of this counter over a given window
/// The database only keep history based on retention.
#[async_trait]
pub trait RRD<S, I, 'a>
where
    S: Slot,
    I: Iterator,
{
    /// Slot returns the current window (slot) to store values.
    async fn slot(&'a mut self) -> Result<S>;
    /// Counters, return all stored counters since the given time (since) until now.
    async fn counters(&self, since: std::time::SystemTime) -> Result<I>;
    /// Last returns the last reported value for a metric given the metric
    /// name
    async fn last(&self, key: &str) -> Result<Option<f64>>;
}

/// SqliteRRD is the [`RRD`] implementation using Sqlite under the hood.
pub struct SqliteRRD {
    pool: sqlx::Pool<Sqlite>,
    window: i64,
    retention: i64,
}

/// SqliteSlot is the [`Slot`] implementation using Sqlite under the hood.
pub struct SqliteSlot<'a> {
    rrd: &'a mut SqliteRRD,
    key: i64,
}

struct Counters {
    index: usize,
    inner: Vec<Counter>,
}

pub struct Counter {
    metric: String,
    value: f64,
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Counter {
            metric: self.metric.clone(),
            value: self.value,
        }
    }
}

impl Iterator for Counters {
    type Item = Counter;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.inner.len() {
            return None;
        }
        let ret = self.inner[self.index].clone();
        self.index += 1;
        Some(ret)
    }
}

impl From<Vec<(String, f64)>> for Counters {
    fn from(v: Vec<(String, f64)>) -> Self {
        let mut inner = Vec::new();
        for r in v {
            inner.push(Counter {
                metric: r.0,
                value: r.1,
            })
        }
        Counters { index: 0, inner }
    }
}

#[async_trait]
impl<'a> Slot for SqliteSlot<'a> {
    async fn counter(&mut self, key: &str, value: f64) -> Result<()> {
        let mut connection = self.rrd.pool.acquire().await?;
        let last = self.rrd.get_last(key).await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        self.rrd.set_last(now, key, &value).await?;
        if last.is_none() {
            return Ok(());
        }
        let last = last.unwrap();
        let diff = if value >= last {
            value - last
        } else {
            // this is either an overflow
            // or counter has been reset (node was restarted hence)
            // metrics are counting from 0 again.
            // hence it's safer to assume diff is just the value
            // reported
            value
        };
        sqlx::query("REPLACE INTO usage (timestamp, metric, value) VALUES (?, ?, ?);")
            .bind(self.key)
            .bind(key)
            .bind(diff)
            .execute(&mut connection)
            .await?;

        Ok(())
    }

    async fn key(&self) -> Result<i64> {
        let k = self.key;
        Ok(k)
    }
}

#[async_trait]
impl<'a> RRD<SqliteSlot<'a>, Counters, 'a> for SqliteRRD {
    async fn last(&self, metric: &str) -> Result<Option<f64>> {
        Ok(self.get_last(metric).await?)
    }

    async fn counters(&self, since: std::time::SystemTime) -> Result<Counters> {
        let mut connection = self.pool.acquire().await?;
        let mut ts = since.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
        ts = (ts / self.window) * self.window;
        let records: Vec<(String, f64)> = sqlx::query_as(
            "SELECT metric, SUM(value) FROM usage WHERE timestamp >= ? GROUP BY metric ;",
        )
        .bind(ts)
        .fetch_all(&mut connection)
        .await?;
        Ok(records.into())
    }

    async fn slot(&'a mut self) -> Result<SqliteSlot<'a>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let slot = self.slot_at(now).await?;
        Ok(slot)
    }
}

impl SqliteRRD {
    /// new creates a new rrd database that uses sqlite as storage. if window or retention are 0
    /// the function will return an RRDError. If retention is smaller then window the function will return an RRDError.
    /// retention and window must be multiple of 1 minute.
    pub async fn new<P: AsRef<Path>>(
        path: P,
        window: time::Duration,
        retention: time::Duration,
    ) -> Result<SqliteRRD> {
        if window.is_zero() {
            anyhow::bail!(Error::InvalidWindow)
        }
        if retention.is_zero() || retention < window {
            anyhow::bail!(Error::InvalidRetention)
        }

        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .create_if_missing(true)
            .filename(path);
        let pool = sqlx::SqlitePool::connect_with(options).await?;
        let mut connection = pool.acquire().await?;

        sqlx::query::<Sqlite>(
            "CREATE TABLE IF NOT EXISTS usage (
                timestamp INTEGER NOT NULL, 
                metric TEXT NOT NULL, 
                value FLOAT NOT NULL,
                PRIMARY KEY (timestamp, metric)
                );",
        )
        .execute(&mut connection)
        .await?;

        sqlx::query::<Sqlite>(
            "CREATE TABLE IF NOT EXISTS last (
                timestamp INTEGER NOT NULL, 
                metric TEXT NOT NULL UNIQUE, 
                value FLOAT NOT NULL,
                PRIMARY KEY (timestamp, metric)
                );",
        )
        .execute(&mut connection)
        .await?;

        sqlx::query::<Sqlite>("CREATE INDEX IF NOT EXISTS ts_index ON usage (timestamp);")
            .execute(&mut connection)
            .await?;

        sqlx::query::<sqlx::Sqlite>("CREATE INDEX IF NOT EXISTS ts_index ON last (timestamp);")
            .execute(&mut connection)
            .await?;

        Ok(SqliteRRD {
            pool,
            retention: retention.as_secs() as i64,
            window: window.as_secs() as i64,
        })
    }

    async fn print<W: Write>(&mut self, mut writer: W) -> Result<W> {
        self.print_last_usage(&mut writer).await?;
        let mut connection = self.pool.acquire().await?;
        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage;")
            .fetch_all(&mut connection)
            .await?;
        for ts in timestamps {
            self.print_ts(ts, &mut writer).await?;
        }
        Ok(writer)
    }

    async fn print_last_usage<W: Write>(&mut self, mut writer: W) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        writer.write_fmt(format_args!(".last\n"))?;
        let records: Vec<(String, f64)> = sqlx::query_as("SELECT metric, usage FROM last;")
            .fetch_all(&mut connection)
            .await?;
        for (metric, usage) in records {
            writer.write_fmt(format_args!("\t{}: {}\n", metric, usage))?
        }
        Ok(())
    }

    async fn print_ts<W: Write>(&mut self, ts: i64, mut writer: W) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        let records: Vec<(String, f64)> =
            sqlx::query_as("SELECT metric, usage FROM usage WHERE timestamp = ? ;")
                .bind(ts)
                .fetch_all(&mut connection)
                .await?;
        for (metric, usage) in records {
            writer.write_fmt(format_args!("\t{}: {}\n", metric, usage))?
        }
        Ok(())
    }

    /// retain deletes any values recorded before some duration greater than or equal to retention.
    async fn retain(&self, now: i64) -> Result<()> {
        // should retain be unsigned?
        let mut connection = self.pool.acquire().await?;
        let retain = (now - self.retention) as i64;
        sqlx::query("DELETE FROM usage WHERE timestamp <= ? ;")
            .bind(retain)
            .execute(&mut connection)
            .await?;
        Ok(())
    }

    /// slots retreives unique timestamps of recordings.
    pub async fn slots(&mut self) -> Result<Vec<i64>> {
        let mut connection = self.pool.acquire().await?;
        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage ;")
            .fetch_all(&mut connection)
            .await?;
        Ok(timestamps)
    }

    /// slots_at returns an [`SqliteSlot`] at some timestamp.
    async fn slot_at<'a>(&'_ mut self, ts: i64) -> Result<SqliteSlot<'_>> {
        let ts = (ts / self.window) * self.window;
        self.retain(ts).await?;
        Ok(SqliteSlot { rrd: self, key: ts })
    }

    /// get_last returns the last value recorded for some metric.
    async fn get_last(&self, key: &str) -> Result<Option<f64>> {
        let mut connection = self.pool.acquire().await?;
        let last: Option<f64> = sqlx::query_scalar("SELECT value FROM last WHERE metric = ? ;")
            .bind(key)
            .fetch_optional(&mut connection)
            .await?;
        Ok(last)
    }

    /// set_last sets or overwrites the last value for some metric at a timestamp.
    pub async fn set_last(&mut self, timestamp: i64, metric: &str, value: &f64) -> Result<()> {
        let mut connection = self.pool.acquire().await?;
        sqlx::query("REPLACE INTO last (timestamp, metric, value) VALUES (?, ?, ?);")
            .bind(timestamp)
            .bind(metric)
            .bind(value)
            .execute(&mut connection)
            .await?;
        Ok(())
    }
}

#[cfg(test)]

mod test {
    use super::Slot;
    use super::RRD;
    use rand::Rng;
    use std::time::{self, SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn add_slot() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = 60 * time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut slot = db.slot().await.unwrap();
        let key = slot.key().await.unwrap();
        let w = window.as_secs() as i64;
        assert_eq!((now / w) * w, key);
        slot.counter("test1", 1234.5).await.unwrap();
    }

    #[tokio::test]
    async fn counters_two_values() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let before_window = now_secs - window.as_secs() as i64;
        let mut slot_before = db.slot_at(before_window).await.unwrap();
        slot_before.counter("test-1", 100.0).await.unwrap();
        let mut slot_now = db.slot_at(now_secs).await.unwrap();
        slot_now.counter("test-1", 120.0).await.unwrap();
        let mut counters = db
            .counters(now.checked_sub(window * 5).unwrap())
            .await
            .unwrap();
        let counter = counters.next().unwrap();
        assert_eq!(counter.value, 20.0);
        assert!(counters.next().is_none());
    }

    #[tokio::test]
    async fn counters_series() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let first = now_secs - 20 * window.as_secs() as i64;
        for i in 0..20 {
            let ts = first + i * 60;
            let mut slot = db.slot_at(ts).await.unwrap();
            slot.counter("test-1", i as f64).await.unwrap();
        }
        let mut counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 10).unwrap())
            .await
            .unwrap();
        let counter = counters.next().unwrap();
        assert_eq!(counter.value, 10.0);
        assert!(counters.next().is_none());
    }

    #[tokio::test]
    async fn counters_random_increase() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let first = now_secs - 5 * window.as_secs() as i64;
        let mut expected: f64 = 0.0;
        for i in 0..5 {
            let mut slot = db.slot_at(first + i * 60).await.unwrap();
            let inc: f64 = rand::thread_rng().gen_range(0.0..10.0);
            if i != 0 {
                expected += inc;
            }
            slot.counter("test-1", expected).await.unwrap();
        }
        let mut counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 10).unwrap())
            .await
            .unwrap();
        let counter = counters.next().unwrap();
        assert_eq!(counter.value, expected);
        assert!(counters.next().is_none());
    }

    #[tokio::test]
    async fn counters_gap() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut slot1 = db.slot_at(now_secs - 3 * 60).await.unwrap();
        slot1.counter("test-1", 100.0).await.unwrap();
        let mut slot_now = db.slot_at(now_secs).await.unwrap();
        slot_now.counter("test-1", 120.0).await.unwrap();
        let mut counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 5).unwrap())
            .await
            .unwrap();
        let counter = counters.next().unwrap();
        assert_eq!(counter.value, 20.0);
        assert!(counters.next().is_none());
    }

    #[tokio::test]
    async fn counters_retention() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let first = now_secs - 20 * window.as_secs() as i64;
        for i in 0..21 {
            let ts = first + i * 60;
            let mut slot = db.slot_at(ts).await.unwrap();
            slot.counter("test-1", i as f64).await.unwrap();
        }
        let slots = db.slots().await.unwrap();
        assert_eq!(slots.len(), 10);
        assert_eq!(((now_secs - 60 * 9) / 60) * 60, slots[0]);
    }

    #[tokio::test]
    async fn counters_last() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let last = db.last("test-1").await.unwrap();
        assert!(last.is_none());
        let mut slot1 = db.slot_at(now_secs - 5 * 60).await.unwrap();
        slot1.counter("test-1", 100.0).await.unwrap();
        let mut slot2 = db.slot_at(now_secs - 2 * 60).await.unwrap();
        slot2.counter("test-1", 120.0).await.unwrap();
        let last = db.last("test-1").await.unwrap();
        assert!(last.is_some());
        assert_eq!(last.unwrap(), 120.0);
    }

    #[tokio::test]
    async fn counters_multiple_reports() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60) * 5;
        let retention = 24 * 60 * time::Duration::from_secs(60);
        let mut db = crate::rrd::SqliteRRD::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut last_report_time = now_secs;
        let mut slot1 = db.slot_at(last_report_time - 5 * 60).await.unwrap();
        slot1.counter("test-0", 0.0).await.unwrap();
        let mut total: f64 = 0.0;
        for i in 0..25 {
            if i % 6 == 0 && i != 0 {
                let mut counters = db
                    .counters(UNIX_EPOCH + time::Duration::from_secs(last_report_time as u64))
                    .await
                    .unwrap();
                let counter = counters.next().unwrap();
                assert!(counters.next().is_none());
                assert_eq!(counter.value, 6.0);
                total += counter.value;
            }
            let mut slot = db.slot_at(now_secs + 60 * 5 * i).await.unwrap();
            slot.counter("test-0", (i as f64) + 1.0).await.unwrap();
            if i % 6 == 0 && i != 0 {
                last_report_time = slot.key().await.unwrap();
            }
        }
        assert_eq!(24.0, total);
    }
}
