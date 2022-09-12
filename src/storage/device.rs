use crate::system::Executor;
use anyhow::Result;
use std::path::PathBuf;

pub trait Device {
    fn path(&self) -> Result<PathBuf>;
    // todo: add more accessor methods
}

#[async_trait::async_trait]
pub trait DeviceManager {
    type Device: Device;

    /// list all devices
    async fn devices(&self) -> Result<Vec<Self::Device>>;
}

pub struct LsblkDevice {
    // hold data here
}

impl Device for LsblkDevice {
    fn path(&self) -> Result<PathBuf> {
        unimplemented!()
    }
}

pub struct LsBlk<E>
where
    E: Executor,
{
    exec: E,
}

impl<E> LsBlk<E>
where
    E: Executor,
{
    fn new(exec: E) -> Self {
        LsBlk { exec }
    }
}

impl Default for LsBlk<crate::system::System> {
    fn default() -> Self {
        LsBlk {
            exec: crate::system::System,
        }
    }
}

#[async_trait::async_trait]
impl<E> DeviceManager for LsBlk<E>
where
    E: Executor + Send + Sync,
{
    type Device = LsblkDevice;

    async fn devices(&self) -> Result<Vec<Self::Device>> {
        unimplemented!("lsblk listing devices")
    }
}

#[cfg(test)]
mod test {
    use super::{DeviceManager, LsBlk};

    #[test]
    fn default() {
        let _ = LsBlk::default();
    }

    #[tokio::test]
    async fn lsblk() {
        let exec = crate::system::ExecutorMock;
        let lsblk = LsBlk::new(exec);

        let devices = lsblk.devices().await;
    }
}
