use crate::Unit;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};
use thiserror::Error;

pub mod device;
pub mod manager;
pub mod mount;
pub mod pool;

pub use mount::{mountinfo, mountpoint, mounts, Mount};

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    Volume,
    Disk,
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Volume => write!(f, "volume"),
            Self::Disk => write!(f, "disk"),
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("object {kind}({id}) not found")]
    NotFound { id: String, kind: Kind },

    #[error("no enough space left on devices")]
    NoEnoughSpaceLeft,

    #[error("invalid size cannot be '{size}'")]
    InvalidSize { size: Unit },

    #[error("{0}")]
    PoolError(#[from] pool::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Usage {
    pub size: Unit,
    pub used: Unit,
}

impl Usage {
    // enough for return true if requested size can fit
    // inside this device. basically means that
    // self.used + size <= self.size
    pub fn enough_for(&self, size: Unit) -> bool {
        self.used + size < self.size
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub name: String,
    pub path: PathBuf,
}

impl<T> From<&T> for VolumeInfo
where
    T: pool::Volume,
{
    fn from(v: &T) -> Self {
        VolumeInfo {
            name: v.name().into(),
            path: v.path().into(),
        }
    }
}

#[async_trait::async_trait]
pub trait Manager {
    /// list all available volumes information
    async fn volumes(&self) -> Result<Vec<VolumeInfo>>;
    /// look up volume by name
    async fn volume_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<VolumeInfo>;
    /// create a new volume with given size
    async fn volume_create<S: AsRef<str> + Send + Sync>(
        &mut self,
        name: S,
        size: Unit,
    ) -> Result<VolumeInfo>;
    /// delete volume by name. If volume not found, return Ok
    async fn volume_delete<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<()>;
}
