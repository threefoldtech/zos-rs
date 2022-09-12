use crate::Unit;
use anyhow::Result;
use std::path::Path;

pub mod lsblk;
pub use lsblk::{LsBlk, LsblkDevice};

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
    type Device: Device;

    /// list all devices
    async fn devices(&self) -> Result<Vec<Self::Device>>;

    async fn device<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Device>;
}
