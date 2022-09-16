use crate::Unit;
/// a pool is a wrapper around a disk device. right now a single pool
/// uses a single disk device.
use std::path::{Path, PathBuf};
use thiserror::Error;
// define error type?

pub mod btrfs;

#[derive(Error, Debug)]
pub enum Error {
    #[error("volume not found {volume}")]
    VolumeNotFound { volume: String },
    #[error("pool not found {pool}")]
    PoolNotFound { pool: String },
    #[error("invalid device {device}")]
    InvalidDevice { device: PathBuf },
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

type Result<T> = anyhow::Result<T, Error>;

#[derive(Debug)]
pub struct Usage {
    pub size: Unit,
    pub used: Unit,
}
/// Volume type.
#[async_trait::async_trait]
pub trait Volume<'a> {
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
pub trait UpPool<'a> {
    /// DownPool is the type returned by (down) operation
    type DownPool: DownPool<'a>;

    /// Volume is associated volume type
    type Volume: Volume<'a>;

    /// path to the mounted pool
    fn path(&self) -> &Path;

    /// name of the pool
    fn name(&self) -> &str;

    /// usage of the pool
    async fn usage(&self) -> Result<Usage>;

    /// down bring the pool down and return a DownPool
    async fn down(self) -> Result<Self::DownPool>;

    /// create a volume
    async fn volume_create<S: AsRef<str> + Send>(&'a self, name: S) -> Result<Self::Volume>;

    /// list all volumes in the pool
    async fn volumes(&'a self) -> Result<Vec<Self::Volume>>;

    /// delete volume pools
    async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()>;
}

#[async_trait::async_trait]
pub trait DownPool<'a> {
    type UpPool: UpPool<'a>;

    async fn up(self) -> Result<Self::UpPool>;
}

pub enum Pool<U, D>
where
    U: UpPool<'static>,
    D: DownPool<'static>,
{
    Up(U),
    Down(D),
}
