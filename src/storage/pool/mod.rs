/// a pool is a wrapper around a disk device. right now a single pool
/// uses a single disk device.
use crate::Unit;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub mod btrfs;
pub use btrfs::BtrfsManager;

use super::device::DeviceManager;

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
    #[error("pool not found {pool}")]
    PoolNotFound { pool: String },
    #[error("invalid device {device}: {reason}")]
    InvalidDevice {
        device: PathBuf,
        reason: InvalidDevice,
    },
    #[error("invalid filesystem on device {device}")]
    InvalidFilesystem { device: PathBuf },
    #[error("invalid volume {volume}")]
    InvalidVolume { volume: PathBuf },
    #[error("volume does not have associated qgroup")]
    QGroupNotFound { volume: PathBuf },
    #[error("operation not support")]
    Unsupported,

    #[error("external operation failed with error: {0}")]
    SystemError(#[from] crate::system::Error),
    //todo: add more errors based on progress
    // cover it all error
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = anyhow::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Usage {
    pub size: Unit,
    pub used: Unit,
}
/// Volume type.
#[async_trait::async_trait]
pub trait Volume {
    /// numeric id of the volume
    fn id(&self) -> u64;

    /// full path to the volume
    fn path(&self) -> &Path;

    /// name of the volume
    fn name(&self) -> &str;

    /// limit, set, update, or remove size limit of the volume
    async fn limit(&self, size: Option<Unit>) -> Result<()>;

    /// usage return size reserved or allocated by a volume
    /// usually if limit is set, the limit is returned as usage
    /// if no limit set, actual files size will be returned
    async fn usage(&self) -> Result<Usage>;
}

/// UpPool is trait for a pool that is hooked to the system and accessible
#[async_trait::async_trait]
pub trait UpPool {
    /// DownPool is the type returned by (down) operation
    type DownPool: DownPool;

    /// Volume is associated volume type
    type Volume: Volume;

    /// path to the mounted pool
    fn path(&self) -> &Path;

    /// name of the pool
    fn name(&self) -> &str;

    /// usage of the pool
    async fn usage(&self) -> Result<Usage>;

    /// down bring the pool down and return a DownPool
    async fn down(self) -> Result<Self::DownPool>;

    /// create a volume
    async fn volume_create<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume>;

    /// list all volumes in the pool
    async fn volumes(&self) -> Result<Vec<Self::Volume>>;

    /// delete volume pools
    async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()>;
}

#[async_trait::async_trait]
pub trait DownPool {
    type UpPool: UpPool;

    async fn up(self) -> Result<Self::UpPool>;

    fn name(&self) -> &str;
}

pub enum Pool<U, D>
where
    U: UpPool,
    D: DownPool,
{
    /// Up pool state
    Up(U),
    /// Down pool stat
    Down(D),
    // /// the none value is used as a place holder
    // /// to be used with mem::replace or mem::swap
    // None,
}

impl<U, D> Pool<U, D>
where
    U: UpPool,
    D: DownPool,
{
    /// return the name of the pool
    pub fn name(&self) -> &str {
        match self {
            Self::Up(up) => up.name(),
            Self::Down(down) => down.name(),
            //Self::None => unimplemented!(), //shouldn't happen
        }
    }
}

impl<U, D> Display for Pool<U, D>
where
    U: UpPool,
    D: DownPool,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up(up) => write!(f, "up({})", up.name()),
            Self::Down(down) => write!(f, "down({})", down.name()),
        }
    }
}

#[async_trait::async_trait]
pub trait PoolManager<M, U, D>
where
    M: DeviceManager,
    U: UpPool,
    D: DownPool,
{
    async fn get(&self, manager: &M, device: M::Device) -> Result<Pool<U, D>>;
}
