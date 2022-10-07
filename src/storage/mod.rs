use crate::Unit;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, path::PathBuf};
use thiserror::Error;

pub mod device;
pub mod manager;
pub mod mount;
pub mod pool;

pub use manager::StorageManager;
pub use mount::{mountinfo, mountpoint, mounts, Mount};

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    Volume,
    Disk,
    Device,
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Volume => write!(f, "volume"),
            Self::Disk => write!(f, "disk"),
            Self::Device => write!(f, "device"),
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("object {kind}({id}) not found")]
    NotFound { id: String, kind: Kind },

    #[error("no enough space left on devices")]
    NoEnoughSpaceLeft,

    #[error("no device left to support required size")]
    NoDeviceLeft,

    #[error("invalid size cannot be '{size}'")]
    InvalidSize { size: Unit },

    #[error("pool error: {0:#}")]
    Pool(#[from] pool::Error),

    #[error("io error: {0:#}")]
    IO(#[from] std::io::Error),

    #[error("unknown error: {0:#}")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInfo {
    pub path: PathBuf,
    pub size: Unit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: String,
    pub path: PathBuf,
    pub size: Unit,
}

#[async_trait::async_trait]
pub trait Manager {
    /// list all available volumes information
    async fn volumes(&self) -> Result<Vec<VolumeInfo>>;
    /// look up volume by name
    async fn volume_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<VolumeInfo>;
    /// create a new volume with given size. if volume already exist, volume
    /// is returned (and size does not change)
    async fn volume_create<S: AsRef<str> + Send + Sync>(
        &mut self,
        name: S,
        size: Unit,
    ) -> Result<VolumeInfo>;
    /// delete volume by name. If volume not found, return Ok
    async fn volume_delete<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<()>;

    /// list all available disks
    async fn disks(&self) -> Result<Vec<DiskInfo>>;

    /// look up disk by name
    async fn disk_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<DiskInfo>;

    /// create a disk with given name and size
    async fn disk_create<S: AsRef<str> + Send + Sync>(
        &mut self,
        name: S,
        size: Unit,
    ) -> Result<DiskInfo>;

    /// delete disk with name
    async fn disk_delete<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<()>;

    /// expand disk to given size which must be bigger than previous size
    async fn disk_expand<S: AsRef<str> + Send + Sync>(&self, name: S, size: Unit) -> Result<()>;

    /// list all allocated devices
    async fn devices(&self) -> Result<Vec<DeviceInfo>>;

    /// look up device by name
    async fn device_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<DeviceInfo>;

    /// device allocate takes the first free HDD that can fullfil the given min size
    async fn device_allocate(&mut self, min: Unit) -> Result<DeviceInfo>;
}
