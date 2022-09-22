use crate::Unit;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;

pub mod lsblk;
pub use lsblk::{LsBlk, LsblkDevice};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum DeviceType {
    #[serde(alias = "hdd")]
    HDD,
    #[serde(alias = "ssd")]
    SSD,
}

impl Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SSD => write!(f, "ssd"),
            Self::HDD => write!(f, "hdd"),
        }
    }
}

impl FromStr for DeviceType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ssd" => Ok(Self::SSD),
            "hdd" => Ok(Self::HDD),
            _ => Err("invalid device type"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Filesystem {
    Btrfs,
}

impl Display for Filesystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Btrfs => write!(f, "btrfs"),
        }
    }
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

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::Unit;
    use std::path::PathBuf;

    #[derive(Clone)]
    pub struct TestDevice {
        pub path: PathBuf,
        pub size: Unit,
        pub filesystem: Option<String>,
        pub label: Option<String>,
        pub device_type: DeviceType,
    }

    impl Device for TestDevice {
        fn path(&self) -> &Path {
            &self.path
        }

        fn size(&self) -> Unit {
            self.size
        }

        fn subsystems(&self) -> &str {
            "device:test"
        }

        fn label(&self) -> Option<&str> {
            self.label.as_ref().map(|s| s.as_str())
        }

        fn filesystem(&self) -> Option<&str> {
            self.filesystem.as_ref().map(|f| f.as_str())
        }

        fn rota(&self) -> bool {
            false
        }
    }

    pub struct TestManager {
        pub devices: Vec<TestDevice>,
    }

    #[async_trait::async_trait]
    impl DeviceManager for TestManager {
        type Device = TestDevice;

        async fn devices(&self) -> Result<Vec<Self::Device>> {
            Ok(self.devices.clone())
        }

        async fn device<P: AsRef<Path> + Send>(&self, _path: P) -> Result<Self::Device> {
            unimplemented!()
        }

        async fn labeled<S: AsRef<str> + Send>(&self, _label: S) -> Result<Self::Device> {
            unimplemented!()
        }

        async fn shutdown(&self, _device: &Self::Device) -> Result<()> {
            unimplemented!()
        }

        async fn seektime(&self, device: &Self::Device) -> Result<DeviceType> {
            Ok(device.device_type.clone())
        }

        async fn format(
            &self,
            mut device: Self::Device,
            filesystem: Filesystem,
            _force: bool,
        ) -> Result<Self::Device> {
            //todo: handle force
            device.filesystem = Some(filesystem.to_string());
            device.label = Some(uuid::Uuid::new_v4().hyphenated().to_string());

            Ok(device)
        }
    }
}
