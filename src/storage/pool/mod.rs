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

    #[error("operation not support")]
    Unsupported,

    #[error("external operation failed with error: {0}")]
    ExternalOperation(#[from] crate::system::Error),
    //todo: add more errors based on progress
    // cover it all error
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

type Result<T> = anyhow::Result<T, Error>;

#[async_trait::async_trait]
pub trait Volume {
    fn id(&self) -> u64;

    fn path(&self) -> &Path;

    fn name(&self) -> &str;

    async fn limit(&self, size: Unit) -> Result<()>;

    async fn usage(&self) -> Result<Unit>;
}

#[async_trait::async_trait]
pub trait Pool: Volume {
    type Volume: Volume;

    async fn mount(&self) -> Result<PathBuf>;

    async fn unmount(&self) -> Result<()>;

    async fn volumes(&self) -> Result<Vec<Self::Volume>>;

    async fn volume<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume>;

    async fn delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()>;
}
