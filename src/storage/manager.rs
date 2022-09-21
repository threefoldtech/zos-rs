use crate::cache::Store;
use crate::storage::device::{DeviceManager, DeviceType};
use crate::storage::pool::{DownPool, UpPool, Volume};
use crate::system::{MsFlags, Syscalls, System};
use crate::Unit;
use anyhow::{Context, Result};
use std::collections::HashMap;

use super::device::{Device, Filesystem};
use super::pool::{Pool, PoolManager};

const FORCE_FORMAT: bool = false;
const CACHE_VOLUME: &str = "zos-cache";
const CACHE_TARGET: &str = "/var/cache";

pub struct Manager<M, P, U, D>
where
    M: DeviceManager,
    U: UpPool,
    D: DownPool,
    P: PoolManager<M::Device, U, D>,
{
    device_mgr: M,
    pool_mgr: P,
    ssds: HashMap<String, Pool<U, D>>,
    hdds: HashMap<String, Pool<U, D>>,
    cache: Store<DeviceType>,
    ssd_size: Unit,
    hdd_size: Unit,
}

impl<M, P, U, D> Manager<M, P, U, D>
where
    M: DeviceManager,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
    P: PoolManager<M::Device, U, D>,
{
    pub async fn new(device_mgr: M, pool_mgr: P) -> Result<Self> {
        let mut this = Self {
            device_mgr,
            pool_mgr,
            ssds: HashMap::default(),
            hdds: HashMap::default(),
            cache: Store::new("storage", 1 * crate::MEGABYTE)
                .await
                .context("failed to initialize storage disk type cache")?,
            ssd_size: 0,
            hdd_size: 0,
        };

        this.initialize().await?;

        // setup cache partition
        Ok(this)
    }

    fn is_formatted(&self, device: &M::Device) -> bool {
        return matches!(device.filesystem(), Some(f) if f == "btrfs") && device.label().is_some();
    }

    async fn get_type(&self, device: &M::Device) -> Result<DeviceType> {
        // first check cache
        let name = match device.path().file_name() {
            Some(name) => name,
            None => anyhow::bail!("invalid device path {:?}", device.path()),
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

    async fn prepare(&self, device: M::Device) -> Result<M::Device> {
        if self.is_formatted(&device) {
            Ok(device)
        } else if FORCE_FORMAT {
            self.device_mgr
                .format(device, Filesystem::Btrfs, FORCE_FORMAT)
                .await
        } else {
            anyhow::bail!(
                "device {:?} has a different filesystem, skipped",
                device.path()
            );
        }
    }

    // search all ssd storage for a volume with given name
    async fn find_volume<S: AsRef<str>>(&self, name: S) -> Result<Option<impl Volume>> {
        // search mounted pool first
        for (_, pool) in self.ssds.iter() {
            let up = match pool {
                Pool::Up(ref up) => up,
                _ => continue,
            };

            let vol = up
                .volumes()
                .await?
                .into_iter()
                .filter(|v| v.name() == name.as_ref())
                .next();

            if let Some(vol) = vol {
                return Ok(Some(vol));
            }
        }

        Ok(None)
    }

    async fn ensure_cache(&self) -> Result<()> {
        let mnt = super::mountpoint(CACHE_TARGET)
            .await
            .context("failed to check mount for cache")?;

        if mnt.is_some() {
            return Ok(());
        }

        let vol = match self.find_volume(CACHE_VOLUME).await? {
            Some(vol) => vol,
            None => unimplemented!(), // create a volume
        };

        System.mount(
            Some(vol.path()),
            CACHE_TARGET,
            Option::<&str>::None,
            MsFlags::MS_BIND,
            Option::<&str>::None,
        )?;

        Ok(())
    }

    async fn initialize(&mut self) -> Result<()> {
        let devices = self.device_mgr.devices().await?;
        for device in devices {
            let device = match self.prepare(device).await {
                Ok(device) => device,
                Err(err) => {
                    log::error!("failed to prepare device duo to: {}", err);
                    continue;
                }
            };

            let device_typ = match self.get_type(&device).await {
                Ok(typ) => typ,
                Err(err) => {
                    log::error!(
                        "failed to detect device '{:?}' type: {}",
                        device.path(),
                        err
                    );
                    continue;
                }
            };

            let pool = match self.pool_mgr.get(device).await {
                Ok(pool) => pool,
                Err(err) => {
                    log::error!("failed to initialize pool for device: {}", err);
                    // store error for reference ?
                    continue;
                }
            };

            // we need to bring the pool up to calculate the size
            let up = match pool {
                Pool::Up(up) => up,
                Pool::Down(down) => {
                    // bring up first.
                    down.up().await?
                }
                Pool::None => {
                    unreachable!()
                }
            };

            let usage = up.usage().await?;

            let pool = if up.volumes().await?.len() == 0 {
                Pool::Down(up.down().await?)
            } else {
                Pool::Up(up)
            };

            // todo: clean up hdd disks

            match device_typ {
                DeviceType::SSD => {
                    self.ssd_size += usage.size;
                    self.ssds.insert(pool.name().into(), pool);
                }
                DeviceType::HDD => {
                    self.hdd_size += usage.size;
                    self.hdds.insert(pool.name().into(), pool);
                }
            };
        }

        // not at this point all pools are "created" but not all of them
        // are actually in up state.
        // hence finding, and/or mounting zos-cache
        Ok(())
    }
}
