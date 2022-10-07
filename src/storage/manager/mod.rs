use super::device::Device;
use super::pool;
use super::pool::State;
use super::pool::{Pool, PoolManager};
use super::Result;
use super::{DeviceInfo, DiskInfo, VolumeInfo};
use crate::cache::Store;
use crate::storage::device::{DeviceManager, DeviceType};
use crate::storage::pool::{DownPool, UpPool, Volume};
use crate::Unit;
use anyhow::Context;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use tokio::fs::OpenOptions;

const VDISKS_VOLUME: &str = "vdisks";
const ZDB_VOLUME: &str = "zdb";

pub struct StorageManager<M, P, U, D>
where
    M: DeviceManager,
    P: PoolManager<M, U, D>,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    device_mgr: M,
    pool_mgr: P,
    ssds: Vec<Pool<U, D>>,
    hdds: Vec<Pool<U, D>>,
    cache: Store<DeviceType>,
    ssd_size: Unit,
    hdd_size: Unit,
}

impl<M, P, U, D> StorageManager<M, P, U, D>
where
    M: DeviceManager,
    P: PoolManager<M, U, D>,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    pub async fn new(device_mgr: M, pool_mgr: P) -> Result<Self> {
        let mut this = Self {
            device_mgr,
            pool_mgr,
            ssds: Vec::default(),
            hdds: Vec::default(),
            cache: Store::new("storage", crate::MEGABYTE)
                .await
                .context("failed to initialize storage disk type cache")?,
            ssd_size: 0,
            hdd_size: 0,
        };

        this.initialize().await?;

        // setup cache partition
        Ok(this)
    }

    async fn get_type(&self, device: &M::Device) -> Result<DeviceType> {
        // first check cache
        let name = match device.path().file_name() {
            Some(name) => name,
            None => {
                return Err(pool::Error::InvalidDevice {
                    device: device.path().into(),
                    reason: pool::InvalidDevice::InvalidPath,
                }
                .into())
            }
        };

        if let Some(t) = self.cache.get(name).await? {
            return Ok(t);
        }

        // if not set, then we need to use the seektime to get and set it
        let t = self.device_mgr.seektime(device).await?;
        self.cache.set(name, &t).await.with_context(|| {
            format!("failed to cache detected device type: {:?}", device.path())
        })?;

        Ok(t)
    }

    async fn validate(&self, pool: &mut Pool<U, D>) -> Result<super::Usage> {
        let up = pool.into_up().await?;

        let usage = up.usage().await?;

        if up.volumes().await?.is_empty() {
            pool.into_down().await?;
        }

        Ok(usage)
    }

    async fn initialize(&mut self) -> Result<()> {
        let devices = self.device_mgr.devices().await?;
        for device in devices {
            let device_typ = match self.get_type(&device).await {
                Ok(typ) => typ,
                Err(err) => {
                    log::error!(
                        "failed to detect device '{}' type: {:#}",
                        device.path().display(),
                        err
                    );
                    continue;
                }
            };

            let mut pool = match self.pool_mgr.get(&self.device_mgr, device).await {
                Ok(pool) => pool,
                Err(err) => {
                    log::error!("failed to initialize pool for device: {:#}", err);
                    // store error for reference ?
                    continue;
                }
            };

            let usage = match self.validate(&mut pool).await {
                Ok(usage) => usage,
                Err(err) => {
                    // invalid pool
                    log::error!("failed to validate pool '{}': {:#}", pool.name(), err);
                    // add to broken pools list.
                    continue;
                }
            };

            // todo: clean up hdd disks
            match device_typ {
                DeviceType::SSD => {
                    self.ssd_size += usage.size;
                    self.ssds.push(pool);
                }
                DeviceType::HDD => {
                    self.hdd_size += usage.size;
                    self.hdds.push(pool);
                }
            };
        }

        // not at this point all pools are "created" but not all of them
        // are actually in up state.
        // hence finding, and/or mounting zos-cache
        Ok(())
    }

    // find an pool with free size. possibly bringing some pools up.
    async fn allocate(&mut self, size: Unit) -> Result<&U> {
        let mut index = None;
        for (i, pool) in self.ssds.iter().enumerate() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            let usage = up.usage().await?;
            if usage.enough_for(size) {
                index = Some(i);
                break;
            }
        }

        if let Some(i) = index {
            return Ok(self.ssds[i].as_up());
        }

        // if we reach here then there is no space left in up pools
        // hence down pools need to be tried out.
        for pool in self.ssds.iter_mut() {
            if pool.size() < size || pool.state() == State::Up {
                continue;
            }

            let up = match pool.into_up().await {
                Ok(up) => up,
                Err(err) => {
                    log::error!("failed to bring pool up: {:#}", err);
                    continue;
                }
            };

            return Ok(up);
        }

        Err(super::Error::NoEnoughSpaceLeft)
    }
}

#[async_trait::async_trait]
impl<M, P, U, D> super::Manager for StorageManager<M, P, U, D>
where
    M: DeviceManager,
    P: PoolManager<M, U, D>,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
{
    async fn volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = vec![];
        for pool in self.ssds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            volumes.extend(up.volumes().await?.iter().map(VolumeInfo::from));
        }

        Ok(volumes)
    }

    async fn volume_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<VolumeInfo> {
        for pool in self.ssds.iter() {
            let up = match pool {
                Pool::Up(ref up) => up,
                _ => continue,
            };

            match up.volume(&name).await {
                Ok(vol) => return Ok((&vol).into()),
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => return Err(err.into()),
            }
        }

        Err(super::Error::NotFound {
            id: name.as_ref().into(),
            kind: super::Kind::Volume,
        })
    }

    async fn volume_create<S: AsRef<str> + Send + Sync>(
        &mut self,
        name: S,
        size: Unit,
    ) -> Result<VolumeInfo> {
        if size == 0 {
            return Err(super::Error::InvalidSize { size });
        }

        match self.volume_lookup(&name).await {
            Ok(volume) => return Ok(volume),
            Err(super::Error::NotFound { .. }) => (),
            Err(err) => return Err(err),
        };

        let pool = self.allocate(size).await?;
        let vol = pool.volume_create(name).await?;
        vol.limit(Some(size)).await?;

        Ok((&vol).into())
    }

    async fn volume_delete<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<()> {
        for pool in self.ssds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            match up.volume_delete(&name).await {
                Ok(_) => {
                    // volume was deleted we can return here or just try the rest to make sure
                    // TODO: bring the pool down if there are no more volumes
                }
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => return Err(err.into()),
            };
        }

        Ok(())
    }

    async fn disk_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<DiskInfo> {
        for pool in self.ssds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            let vol: U::Volume = match up.volume(VDISKS_VOLUME).await {
                Ok(vol) => vol,
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => {
                    log::error!(
                        "failed to list volumes from pool '{}': {:#}",
                        up.name(),
                        err
                    );
                    continue;
                }
            };

            //todo: is it a safe path? beware of path injection like "../name"
            let path = vol.path().join(name.as_ref());
            if let Ok(meta) = tokio::fs::metadata(&path).await {
                return Ok(DiskInfo {
                    path,
                    size: meta.len(),
                });
            }
        }

        Err(super::Error::NotFound {
            kind: super::Kind::Disk,
            id: name.as_ref().into(),
        })
    }

    async fn disk_create<S: AsRef<str> + Send + Sync>(
        &mut self,
        name: S,
        size: Unit,
    ) -> Result<DiskInfo> {
        match self.disk_lookup(&name).await {
            Ok(disk) => return Ok(disk),
            Err(super::Error::NotFound { .. }) => (),
            Err(err) => return Err(err),
        };

        //
        let pool = self.allocate(size).await?;
        let vol = match pool.volume(VDISKS_VOLUME).await {
            Ok(vol) => vol,
            Err(pool::Error::VolumeNotFound { .. }) => pool.volume_create(VDISKS_VOLUME).await?,
            Err(err) => return Err(err.into()),
        };

        let path = vol.path().join(name.as_ref());
        mkdisk(&path, size).await?;

        Ok(DiskInfo { path, size })
    }

    async fn disks(&self) -> Result<Vec<DiskInfo>> {
        let mut disks = vec![];
        for pool in self.ssds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            let vol: U::Volume = match up.volume(VDISKS_VOLUME).await {
                Ok(vol) => vol,
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => {
                    log::error!(
                        "failed to list volumes from pool '{}': {:#}",
                        up.name(),
                        err
                    );
                    continue;
                }
            };

            let mut entries = tokio::fs::read_dir(vol.path()).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = vol.path().join(entry.file_name());
                let meta = match entry.metadata().await {
                    Ok(meta) => meta,
                    Err(err) => {
                        log::error!(
                            "failed to get disk information '{}': {:#}",
                            path.display(),
                            err
                        );
                        continue;
                    }
                };

                if !meta.file_type().is_file() {
                    continue;
                }

                disks.push(DiskInfo {
                    path,
                    size: meta.len(),
                });
            }
        }

        Ok(disks)
    }

    async fn disk_delete<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<()> {
        let disk = match self.disk_lookup(&name).await {
            Ok(disk) => disk,
            Err(super::Error::NotFound { .. }) => return Ok(()),
            Err(err) => return Err(err),
        };

        tokio::fs::remove_file(disk.path)
            .await
            .map_err(|err| err.into())
    }

    async fn disk_expand<S: AsRef<str> + Send + Sync>(&self, name: S, size: Unit) -> Result<()> {
        // expand disk size
        let disk = self.disk_lookup(name).await?;

        use std::cmp::Ordering;
        match size.cmp(&disk.size) {
            Ordering::Less => return Err(super::Error::InvalidSize { size }),
            Ordering::Equal => return Ok(()),
            _ => (),
        };

        mkdisk(disk.path, size).await
    }

    // devices
    async fn device_allocate(&mut self, min: Unit) -> Result<DeviceInfo> {
        for pool in self.hdds.iter_mut() {
            if pool.state() == State::Up || pool.size() < min {
                continue;
            }

            let up: &U = pool.into_up().await?;
            // if volume exist with the same name this definitely
            // then be already up. we avoid allocating it anyway
            match up.volume(ZDB_VOLUME).await {
                Ok(_) => continue,
                Err(pool::Error::VolumeNotFound { .. }) => (),
                Err(err) => return Err(err.into()),
            };

            let volume = up.volume_create(ZDB_VOLUME).await?;
            return Ok(DeviceInfo {
                id: up.name().into(),
                path: volume.path().into(),
                size: up.size(),
            });
        }

        Err(super::Error::NoDeviceLeft)
    }

    async fn devices(&self) -> Result<Vec<DeviceInfo>> {
        let mut devices = vec![];
        for pool in self.hdds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            // if volume exist with the same name this definitely
            // then be already up. we avoid allocating it anyway
            let vol = match up.volume(ZDB_VOLUME).await {
                Ok(vol) => vol,
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => {
                    log::error!("failed to get volume: {:#}", err);
                    continue;
                }
            };

            devices.push(DeviceInfo {
                id: up.name().into(),
                path: vol.path().into(),
                size: pool.size(),
            })
        }

        Ok(devices)
    }

    async fn device_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<DeviceInfo> {
        for pool in self.hdds.iter() {
            let up = match pool {
                Pool::Up(up) => up,
                _ => continue,
            };

            if up.name() != name.as_ref() {
                continue;
            }

            // if volume exist with the same name this definitely
            // then be already up. we avoid allocating it anyway
            let vol = match up.volume(ZDB_VOLUME).await {
                Ok(vol) => vol,
                Err(pool::Error::VolumeNotFound { .. }) => continue,
                Err(err) => {
                    log::error!("failed to get volume: {:#}", err);
                    continue;
                }
            };

            return Ok(DeviceInfo {
                id: up.name().into(),
                path: vol.path().into(),
                size: pool.size(),
            });
        }

        Err(super::Error::NotFound {
            id: name.as_ref().into(),
            kind: super::Kind::Device,
        })
    }
}

async fn mkdisk<T: AsRef<Path>>(path: T, size: Unit) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&path)
        .await
        .context("failed to create disk file")?;

    if cfg!(not(test)) {
        let out = unsafe {
            const FS_NOCOW_FL: i64 = 0x00800000;
            ioctls::fs_ioc_setflags(file.as_raw_fd(), &FS_NOCOW_FL)
        };

        if out != 0 {
            return Err(anyhow::anyhow!("failed to set NOCOW flag: {}", out).into());
        }
    };

    use nix::fcntl::FallocateFlags;
    // this is not async
    nix::fcntl::fallocate(file.as_raw_fd(), FallocateFlags::empty(), 0, size as i64)
        .context("failed to allocate required disk size")?;
    Ok(())
}

#[cfg(test)]
mod test;
