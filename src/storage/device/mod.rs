use crate::Unit;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub mod lsblk;
pub use lsblk::{LsBlk, LsblkDevice};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceType {
    #[serde(alias = "hdd")]
    HDD,
    #[serde(alias = "ssd")]
    SSD,
}

pub enum Filesystem {
    Btrfs,
}

pub trait Device {
    fn path(&self) -> &Path;

    fn size(&self) -> Unit;

    fn subsystems(&self) -> &str;

    fn filesystem(&self) -> Option<&str>;

    fn label(&self) -> Option<&str>;

    fn rota(&self) -> bool;
}

#[async_trait::async_trait]
pub trait DeviceManager {
    type Device: Device + Send + Sync;

    /// list all devices
    async fn devices(&self) -> Result<Vec<Self::Device>>;

    async fn device<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Device>;

    async fn labeled<S: AsRef<str> + Send>(&self, label: S) -> Result<Self::Device>;

    async fn shutdown(&self, device: &Self::Device) -> Result<()>;

    async fn seektime(&self, device: &Self::Device) -> Result<DeviceType>;

    async fn format(
        &self,
        device: Self::Device,
        filesystem: Filesystem,
        force: bool,
    ) -> Result<Self::Device>;
}
