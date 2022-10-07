/// a pool is a wrapper around a disk device. right now a single pool
/// uses a single disk device.
use crate::Unit;
use std::fmt::{Debug, Display};
use std::path::{Path, PathBuf};
use thiserror::Error;

use super::device::DeviceManager;
pub use crate::storage::Usage;

pub mod btrfs;
pub use btrfs::BtrfsManager;

#[derive(Debug)]
pub enum InvalidDevice {
    InvalidPath,
    InvalidLabel,
}

impl Display for InvalidDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath => write!(f, "invalid path"),
            Self::InvalidLabel => write!(f, "invalid label"),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("volume not found {volume}")]
    VolumeNotFound { volume: String },
    #[error("volume {volume} already exists")]
    VolumeAlreadyExists { volume: String },
    #[error("pool not found {pool}")]
    PoolNotFound { pool: String },
    #[error("invalid device {device}: {reason}")]
    InvalidDevice {
        device: PathBuf,
        reason: InvalidDevice,
    },
    #[error("invalid filesystem on device {device} ({filesystem})")]
    InvalidFilesystem { device: PathBuf, filesystem: String },
    #[error("invalid volume {volume}")]
    InvalidVolume { volume: PathBuf },
    #[error("volume does not have associated qgroup")]
    QGroupNotFound { volume: PathBuf },
    #[error("operation not support")]
    Unsupported,

    #[error("external operation failed with error: {0:#}")]
    SystemError(#[from] crate::system::Error),
    //todo: add more errors based on progress
    // cover it all error
    #[error("{0:#}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = anyhow::Result<T, Error>;

pub struct UpError<D>
where
    D: DownPool,
{
    pub pool: D,
    pub error: Error,
}

impl<D> Display for UpError<D>
where
    D: DownPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to bring pool {} up: {}",
            self.pool.name(),
            self.error
        )
    }
}

impl<D> Debug for UpError<D>
where
    D: DownPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to bring pool {} up: {}",
            self.pool.name(),
            self.error
        )
    }
}

pub struct DownError<U>
where
    U: UpPool,
{
    pub pool: U,
    pub error: Error,
}

impl<U> Display for DownError<U>
where
    U: UpPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to bring pool {} down: {}",
            self.pool.name(),
            self.error
        )
    }
}

impl<U> Debug for DownError<U>
where
    U: UpPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to bring pool {} down: {}",
            self.pool.name(),
            self.error
        )
    }
}
/// Volume type.
#[async_trait::async_trait]
pub trait Volume: Send + Sync {
    /// numeric id of the volume
    fn id(&self) -> u64;

    /// full path to the volume
    fn path(&self) -> &Path;

    /// name of the volume
    fn name(&self) -> &str;

    /// limit, set, update, or remove size limit of the volume
    async fn limit(&self, size: Option<Unit>) -> Result<()>;

    /// usage return volume limit size (quota). If no quota
    /// set the actual disk usage (by files in the volume) is
    /// returned
    async fn usage(&self) -> Result<Unit>;
}

/// UpPool is trait for a pool that is hooked to the system and accessible
#[async_trait::async_trait]
pub trait UpPool: Sized + Send + Sync {
    /// DownPool is the type returned by (down) operation
    type DownPool: DownPool;

    /// Volume is associated volume type
    type Volume: Volume;

    /// path to the mounted pool
    fn path(&self) -> &Path;

    /// name of the pool
    fn name(&self) -> &str;

    fn size(&self) -> Unit;

    /// usage of the pool
    async fn usage(&self) -> Result<Usage>;

    /// down bring the pool down and return a DownPool
    async fn down(self) -> std::result::Result<Self::DownPool, DownError<Self>>;

    /// create a volume
    async fn volume_create<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume>;

    /// list all volumes in the pool
    async fn volumes(&self) -> Result<Vec<Self::Volume>>;

    /// delete volume pools
    async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()>;

    async fn volume<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<Self::Volume>;
}

#[async_trait::async_trait]
pub trait DownPool: Sized + Send + Sync {
    type UpPool: UpPool;

    async fn up(self) -> std::result::Result<Self::UpPool, UpError<Self>>;

    fn name(&self) -> &str;

    fn size(&self) -> Unit;
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum State {
    Up,
    Down,
}

#[derive(Clone)]
pub enum Pool<U, D>
where
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    /// Up pool state
    Up(U),
    /// Down pool stat
    Down(D),
    /// the transit value is used as a place holder
    /// during
    Transit,
}

impl<U, D> Pool<U, D>
where
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    /// return the name of the pool
    pub fn name(&self) -> &str {
        match self {
            Self::Up(up) => up.name(),
            Self::Down(down) => down.name(),
            Self::Transit => unreachable!(),
        }
    }

    pub fn size(&self) -> Unit {
        match self {
            Self::Up(up) => up.size(),
            Self::Down(down) => down.size(),
            Self::Transit => unreachable!(),
        }
    }

    pub fn state(&self) -> State {
        match self {
            Self::Up(_) => State::Up,
            Self::Down(_) => State::Down,
            Self::Transit => unreachable!(),
        }
    }

    pub fn as_up(&self) -> &U {
        if let Self::Up(ref p) = self {
            return p;
        }
        panic!("pool is not up")
    }

    pub fn as_down(&self) -> &D {
        if let Self::Down(ref p) = self {
            return p;
        }
        panic!("pool is not down")
    }

    pub async fn into_up(&mut self) -> Result<&U> {
        if self.state() == State::Up {
            return Ok(self.as_up());
        }

        let current = std::mem::replace(self, Self::Transit);
        let down = match current {
            Self::Down(down) => down,
            _ => unreachable!(),
        };

        let up = match down.up().await {
            Ok(up) => up,
            Err(UpError { pool, error }) => {
                let _ = std::mem::replace(self, Self::Down(pool));
                return Err(error);
            }
        };

        let _ = std::mem::replace(self, Self::Up(up));
        Ok(self.as_up())
    }

    pub async fn into_down(&mut self) -> Result<&D> {
        if self.state() == State::Down {
            return Ok(self.as_down());
        }
        let current = std::mem::replace(self, Self::Transit);
        let up = match current {
            Self::Up(up) => up,
            _ => unreachable!(),
        };

        let down = match up.down().await {
            Ok(down) => down,
            Err(DownError { pool, error }) => {
                let _ = std::mem::replace(self, Self::Up(pool));
                return Err(error);
            }
        };

        let _ = std::mem::replace(self, Self::Down(down));
        Ok(self.as_down())
    }
}

impl<U, D> Display for Pool<U, D>
where
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up(up) => write!(f, "up({})", up.name()),
            Self::Down(down) => write!(f, "down({})", down.name()),
            Self::Transit => write!(f, "pool is in transit state"),
        }
    }
}

#[async_trait::async_trait]
pub trait PoolManager<M, U, D>: Send + Sync
where
    M: DeviceManager,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    async fn get(&self, manager: &M, device: M::Device) -> Result<Pool<U, D>>;
}
