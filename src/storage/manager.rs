use crate::cache::Store;
use crate::storage::device::{DeviceManager, DeviceType};
use crate::storage::pool::DefaultBtrfsPool as BtrfsPool;
use anyhow::{Context, Result};
use std::collections::HashMap;

use super::device::{Device, Filesystem};

const FORCE_FORMAT: bool = false;

pub struct Manager<M>
where
    M: DeviceManager,
{
    mgr: M,
    ssds: HashMap<String, BtrfsPool<M::Device>>,
    hdds: HashMap<String, BtrfsPool<M::Device>>,
    cache: Store<DeviceType>,
}

impl<M> Manager<M>
where
    M: DeviceManager,
{
    pub async fn new(mgr: M) -> Result<Self> {
        let mut this = Self {
            mgr,
            ssds: HashMap::default(),
            hdds: HashMap::default(),
            cache: Store::new("storage", 1 * crate::MEGABYTE)
                .await
                .context("failed to initialize storage disk type cache")?,
        };

        this.initialize().await?;

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
        let t = self.mgr.seektime(device).await?;
        self.cache.set(name, &t).await.with_context(|| {
            format!("failed to cache detected device type: {:?}", device.path())
        })?;

        Ok(t)
    }

    async fn prepare(&self, device: M::Device) -> Result<M::Device> {
        if self.is_formatted(&device) {
            Ok(device)
        } else if FORCE_FORMAT {
            self.mgr
                .format(device, Filesystem::Btrfs, FORCE_FORMAT)
                .await
        } else {
            anyhow::bail!(
                "device {:?} has a different filesystem, skipped",
                device.path()
            );
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        let devices = self.mgr.devices().await?;
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

            let pool = match BtrfsPool::new(device).await {
                Ok(pool) => pool,
                Err(err) => {
                    log::error!("failed to initialize pool for device: {}", err);
                    // store error for reference ?
                    continue;
                }
            };

            match device_typ {
                DeviceType::SSD => self.ssds.insert(pool.name().into(), pool),
                DeviceType::HDD => self.hdds.insert(pool.name().into(), pool),
            };
        }

        Ok(())
    }
}
