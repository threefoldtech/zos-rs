use crate::storage::device::DeviceManager;
use crate::storage::pool::DefaultBtrfsPool as BtrfsPool;
use anyhow::Result;
use std::collections::HashMap;

use super::device::{Device, Filesystem};

const FORCE_FORMAT: bool = false;

pub struct Manager<M>
where
    M: DeviceManager,
{
    mgr: M,
    pools: HashMap<String, BtrfsPool<M::Device>>,
}

impl<M> Manager<M>
where
    M: DeviceManager,
{
    pub fn new(mgr: M) -> Self {
        Self {
            mgr,
            pools: HashMap::default(),
        }
    }

    fn is_formatted(&self, device: &M::Device) -> bool {
        return matches!(device.filesystem(), Some(f) if f == "btrfs") && device.label().is_some();
    }

    async fn initialize(&mut self) -> Result<()> {
        let devices = self.mgr.devices().await?;
        for device in devices {
            let device = if self.is_formatted(&device) {
                device
            } else if FORCE_FORMAT {
                self.mgr
                    .format(device, Filesystem::Btrfs, FORCE_FORMAT)
                    .await?
            } else {
                continue;
            };

            let pool = match BtrfsPool::new(device).await {
                Ok(pool) => pool,
                Err(err) => {
                    log::error!("failed to initialize pool for device: {}", err);
                    // store error for reference ?
                    continue;
                }
            };

            self.pools.insert(pool.name().into(), pool);
        }

        unimplemented!()
    }
}
