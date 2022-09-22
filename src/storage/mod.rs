use crate::Unit;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};
use thiserror::Error;

pub mod device;
pub mod manager;
pub mod mount;
pub mod pool;

pub use mount::{mountinfo, mountpoint, Mount};

#[derive(Debug)]
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

    #[error("{0}")]
    PoolError(#[from] pool::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub size: Unit,
    pub used: Unit,
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
    async fn volume_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<VolumeInfo>;
    async fn volumes(&self) -> Result<Vec<VolumeInfo>>;
}
