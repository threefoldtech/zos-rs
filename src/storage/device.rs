use crate::system::Executor;
use anyhow::Result;
use std::path::PathBuf;

pub trait Device {
    fn path(&self) -> Result<PathBuf>;
}

#[async_trait::async_trait]
pub trait DeviceManager {
    type Device: Device;

    /// list all devices
    async fn devices(&self) -> Result<Vec<Self::Device>>;
}

pub struct LsBlk<E>
where
    E: Executor,
{
    exec: E,
}

impl Default for LsBlk<crate::system::System> {
    fn default() -> Self {
        LsBlk {
            exec: crate::system::System,
        }
    }
}

#[cfg(test)]
mod test {
    use super::LsBlk;

    #[test]
    fn default() {
        let _ = LsBlk::default();
    }
}
