use super::{Error, Pool, Result, Volume};
use crate::storage::device::Device;
use crate::Unit;
use std::path::{Path, PathBuf};

pub struct BtrfsVolume {
    path: PathBuf,
}

#[async_trait::async_trait]
impl Volume for BtrfsVolume {
    fn id(&self) -> u64 {
        unimplemented!()
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn name(&self) -> &str {
        self.path
            .file_name()
            .map(|s| s.to_str().unwrap())
            .unwrap_or("unknown")
    }

    async fn limit(&self, size: Unit) -> Result<()> {
        Err(Error::Unsupported)
    }

    async fn usage(&self) -> Result<Unit> {
        unimplemented!()
    }
}

pub struct BtrfsPool<D>
where
    D: Device,
{
    device: D,
    path: PathBuf,
}

#[async_trait::async_trait]
impl<D> Volume for BtrfsPool<D>
where
    D: Device + Send + Sync,
{
    fn id(&self) -> u64 {
        0
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn name(&self) -> &str {
        self.device.label().unwrap_or("unknown")
    }

    async fn limit(&self, size: Unit) -> Result<()> {
        Err(Error::Unsupported)
    }

    async fn usage(&self) -> Result<Unit> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl<D> Pool for BtrfsPool<D>
where
    D: Device + Send + Sync,
{
    type Volume = BtrfsVolume;

    async fn mount(&self) -> Result<&Path> {
        unimplemented!()
    }

    async fn unmount(&self) -> Result<()> {
        unimplemented!()
    }

    async fn volumes(&self) -> Result<Vec<Self::Volume>> {
        unimplemented!()
    }

    async fn volume<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume> {
        unimplemented!()
    }

    async fn delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()> {
        unimplemented!()
    }
}
