use async_trait::async_trait;
use core::time;
use sqlx::{self, Sqlite};
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
    async fn slot(&mut self) -> Result<S>;
    async fn counters(&mut self, since: std::time::SystemTime) -> Result<Vec<(String, f64)>>;
    async fn last(&mut self, key: &str) -> Result<Option<f64>>;
}

pub struct RRDImpl {
    pool: sqlx::Pool<Sqlite>,
    window: i64,
    retention: i64,
}

pub struct RRDSlotImpl {
    connection: sqlx::pool::PoolConnection<Sqlite>,
    key: i64,
}

#[async_trait]
impl Slot for RRDSlotImpl {
    async fn counter(&mut self, key: &str, value: f64) -> Result<()> {
        let last = RRDSlotImpl::get_last(self, key).await?;
        RRDSlotImpl::set_last(self, key, &value).await?;
        if last.is_none() {
            return Ok(());
        }
        let last = last.unwrap();
        let diff: f64;
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
            .bind(self.key)
            .bind(key)
            .bind(diff)
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
    async fn last(&mut self, metric: &str) -> Result<Option<f64>> {
        let mut slot = RRDImpl::slot(self).await?;
        Ok(slot.get_last(metric).await?)
    }

    async fn counters(&mut self, since: std::time::SystemTime) -> Result<Vec<(String, f64)>> {
        let mut connection = self.pool.acquire().await?;
        let ts = since.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64;
        let ts = (ts / self.window) * self.window;

        let records: Vec<(String, f64)> = sqlx::query_as(
            "SELECT metric, SUM(value) FROM usage GROUP BY metric HAVING timestamp >= ? ;",
        )
        .bind(ts)
        .fetch_all(&mut connection)
        .await?;
        Ok(records)
    }

    async fn slot(&mut self) -> Result<RRDSlotImpl> {
        let connection = self.pool.acquire().await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        let ts = (now / self.window) * self.window;
        RRDImpl::retain(&mut self, ts).await?;
        Ok(RRDSlotImpl {
            connection,
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

        Ok(RRDImpl {
            pool,
            retention: retention.as_secs() as i64,
            window: window.as_secs() as i64,
        })
    }

    pub async fn print<W: Write>(&mut self, mut writer: W) -> Result<W> {
        RRDImpl::print_last_usage(self, &mut writer).await?;
        let mut connection = self.pool.acquire().await?;
        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage;")
            .fetch_all(&mut connection)
            .await?;
        for ts in timestamps {
            RRDImpl::print_ts(self, ts, &mut writer).await?;
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

    async fn retain(&mut self, now: i64) -> Result<()> {
        // should retain be unsigned?
        let mut connection = self.pool.acquire().await?;
        let retain = (now - self.retention) as i64;
        sqlx::query("DELETE FROM usage WHERE timestamp <= ? ;")
            .bind(retain)
            .execute(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn slots(&mut self) -> Result<Vec<i64>> {
        let mut connection = self.pool.acquire().await?;
        let timestamps: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT timestamp FROM usage ;")
            .fetch_all(&mut connection)
            .await?;
        Ok(timestamps)
    }

    pub async fn slot_at(&mut self, ts: i64) -> Result<RRDSlotImpl> {
        let connection = self.pool.acquire().await?;
        let ts = (ts / self.window) * self.window;
        RRDImpl::retain(self, ts).await?;
        Ok(RRDSlotImpl {
            connection,
            key: ts,
        })
    }
}

impl RRDSlotImpl {
    async fn get_last(&mut self, key: &str) -> Result<Option<f64>> {
        let last: Vec<f64> = sqlx::query_scalar("SELECT value FROM last WHERE metric = ? ;")
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

#[cfg(test)]

mod test {
    use std::time::{self, SystemTime, UNIX_EPOCH};

    use rand::Rng;

    use super::Slot;
    use super::RRD;

    #[tokio::test]
    async fn add_slot() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = 60 * time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let mut slot = db.slot().await.unwrap();
        let key = slot.key().await.unwrap();
        let w = window.as_secs() as i64;
        assert_eq!(
            (now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 / w) * w,
            key
        );
        slot.counter("test1", 1234.5).await.unwrap();
    }

    #[tokio::test]
    async fn counters_two_values() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let before_window = now
            .checked_sub(window)
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut slot_before = db.slot_at(before_window).await.unwrap();
        let mut slot_now = db
            .slot_at(now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64)
            .await
            .unwrap();
        slot_before.counter("test-1", 100.0).await.unwrap();
        slot_now.counter("test-1", 120.0).await.unwrap();
        let counters = db
            .counters(now.checked_sub(window * 5).unwrap())
            .await
            .unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].1, 20.0);
    }

    #[tokio::test]
    async fn counters_series() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let first = now.checked_sub(window * 20).unwrap();
        for i in 0..20 {
            let ts = first
                .checked_add(time::Duration::from_secs(60) * i)
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let mut slot = db.slot_at(ts).await.unwrap();
            slot.counter("test-1", i as f64).await.unwrap();
        }
        let counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 10).unwrap())
            .await
            .unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].1, 10.0);
    }

    #[tokio::test]
    async fn counters_random_increase() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let first = now.checked_sub(window * 5).unwrap();
        let mut expected: f64 = 0.0;
        for i in 0..5 {
            let mut slot = db
                .slot_at(
                    first
                        .checked_add(time::Duration::from_secs(60) * i)
                        .unwrap()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                )
                .await
                .unwrap();
            let inc: f64 = rand::thread_rng().gen_range(0.0..10.0);
            if i != 0 {
                expected += inc;
            }
            slot.counter("test-1", expected).await.unwrap();
        }
        let counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 10).unwrap())
            .await
            .unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].1, expected);
    }

    #[tokio::test]
    async fn counters_gap() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut slot1 = db
            .slot_at(now_secs - 3 * time::Duration::from_secs(60).as_secs() as i64)
            .await
            .unwrap();
        let mut slot_now = db.slot_at(now_secs).await.unwrap();
        slot1.counter("test-1", 100.0).await.unwrap();
        slot_now.counter("test-1", 120.0).await.unwrap();
        let counters = db
            .counters(now.checked_sub(time::Duration::from_secs(60) * 5).unwrap())
            .await
            .unwrap();
        assert_eq!(counters.len(), 1);
        assert_eq!(counters[0].1, 20.0);
    }

    #[tokio::test]
    async fn counters_retention() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let first = now.checked_sub(window * 20).unwrap();
        for i in 0..21 {
            let ts = first
                .checked_add(time::Duration::from_secs(60) * i)
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let mut slot = db.slot_at(ts).await.unwrap();
            slot.counter("test-1", i as f64).await.unwrap();
        }
        let slots = db.slots().await.unwrap();
        assert_eq!(slots.len(), 10);
        assert_eq!(
            (now.checked_sub(time::Duration::from_secs(60) * 9)
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                / 60)
                * 60,
            slots[0]
        );
    }

    #[tokio::test]
    async fn counters_last() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path();
        let window = time::Duration::from_secs(60);
        let retention = 10 * window;
        let mut db = crate::rrd::RRDImpl::new(path, window, retention)
            .await
            .unwrap();
        let now = SystemTime::now();
        let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let last = db.last("test-1").await.unwrap();
        assert!(last.is_none());
        let mut slot1 = db
            .slot_at(now_secs - 5 * time::Duration::from_secs(60).as_secs() as i64)
            .await
            .unwrap();
        let mut slot2 = db
            .slot_at(now_secs - 2 * time::Duration::from_secs(60).as_secs() as i64)
            .await
            .unwrap();
        slot1.counter("test-1", 100.0).await.unwrap();
        slot2.counter("test-1", 120.0).await.unwrap();
        let last = db.last("test-1").await.unwrap();
        assert!(last.is_some());
        assert_eq!(last.unwrap(), 120.0);
    }

    //     #[tokio::test]
    //     async fn counters_multiple_reports() {
    //         let file = tempfile::NamedTempFile::new().unwrap();
    //         let path = file.path();
    //         let window = time::Duration::from_secs(60) * 5;
    //         let retention = 24 * 60 * time::Duration::from_secs(60);
    //         let mut db = crate::rrd::RRDImpl::new(path, window, retention)
    //             .await
    //             .unwrap();
    //         let now = SystemTime::now();
    //         let mut last_report_time = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    //         let mut slot1 = db
    //             .slot_at(last_report_time - 5 * time::Duration::from_secs(60).as_secs() as i64)
    //             .await
    //             .unwrap();
    //         slot1.counter("test-0", 0.0).await.unwrap();
    //         let mut total: f64 = 0.0;
    //         for i in 0..25 {
    //             let mut slot = db
    //                 .slot_at(
    //                     now.checked_add(time::Duration::from_secs(60) * 5 * i)
    //                         .unwrap()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs() as i64,
    //                 )
    //                 .await
    //                 .unwrap();
    //             if i % 6 == 0 && i != 0 {
    //                 let counters = db
    //                     .counters(UNIX_EPOCH + time::Duration::from_secs(last_report_time as u64))
    //                     .await
    //                     .unwrap();
    //                 assert_eq!(counters.len(), 1);
    //                 last_report_time = slot.key().await.unwrap();
    //                 assert_eq!(counters[0].1, 6.0);
    //                 total += counters[0].1;
    //             }
    //             slot.counter("test-0", (i as f64) + 1.0).await.unwrap();
    //         }
    //         assert_eq!(24.0, total);
    //         todo!()
    //     }
}
