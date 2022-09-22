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
    P: PoolManager<M, U, D>,
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
    P: PoolManager<M, U, D>,
    U: UpPool<DownPool = D>,
    D: DownPool<UpPool = U>,
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

            let pool = match self.pool_mgr.get(&self.device_mgr, device).await {
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

#[cfg(test)]
mod test {

    use super::Manager;
    use crate::storage::device::{Device, DeviceManager};
    use crate::storage::pool::*;
    use crate::Unit;
    use std::path::{Path, PathBuf};

    struct TestUpPool {
        pub name: String,
        pub path: PathBuf,
        pub usage: Usage,
    }
    struct TestDownPool {
        pub name: String,
        pub up: TestUpPool,
    }

    struct TestVolume {
        pub id: u64,
        pub path: PathBuf,
        pub name: String,
        pub usage: Usage,
    }

    #[async_trait::async_trait]
    impl Volume for TestVolume {
        /// numeric id of the volume
        fn id(&self) -> u64 {
            self.id
        }

        /// full path to the volume
        fn path(&self) -> &Path {
            &self.path
        }

        /// name of the volume
        fn name(&self) -> &str {
            &self.name
        }

        /// limit, set, update, or remove size limit of the volume
        async fn limit(&self, size: Option<Unit>) -> Result<()> {
            unimplemented!()
        }

        async fn usage(&self) -> Result<Usage> {
            Ok(self.usage.clone())
        }
    }

    #[async_trait::async_trait]
    impl DownPool for TestDownPool {
        type UpPool = TestUpPool;

        fn name(&self) -> &str {
            &self.name
        }

        async fn up(self) -> Result<Self::UpPool> {
            Ok(self.up)
        }
    }

    #[async_trait::async_trait]
    impl UpPool for TestUpPool {
        type DownPool = TestDownPool;
        type Volume = TestVolume;

        /// path to the mounted pool
        fn path(&self) -> &Path {
            &self.path
        }

        /// name of the pool
        fn name(&self) -> &str {
            &self.name
        }

        /// usage of the pool
        async fn usage(&self) -> Result<Usage> {
            Ok(self.usage.clone())
        }

        /// down bring the pool down and return a DownPool
        async fn down(self) -> Result<Self::DownPool> {
            Ok(TestDownPool {
                name: self.name.clone(),
                up: self,
            })
        }

        /// create a volume
        async fn volume_create<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume> {
            unimplemented!()
        }

        /// list all volumes in the pool
        async fn volumes(&self) -> Result<Vec<Self::Volume>> {
            Ok(vec![])
        }

        /// delete volume pools
        async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()> {
            unimplemented!()
        }
    }

    struct TestPoolManager;
    #[async_trait::async_trait]
    impl<M> PoolManager<M, TestUpPool, TestDownPool> for TestPoolManager
    where
        M: DeviceManager + Send + Sync + 'static,
    {
        async fn get(
            &self,
            _manager: &M,
            device: M::Device,
        ) -> Result<Pool<TestUpPool, TestDownPool>> {
            let name: String = device.path().file_name().unwrap().to_str().unwrap().into();
            Ok(Pool::Down(TestDownPool {
                name: name.clone(),
                up: TestUpPool {
                    name: name.clone(),
                    path: device.path().join(name),
                    usage: Usage {
                        size: device.size(),
                        used: 0,
                    },
                },
            }))
        }
    }

    #[tokio::test]
    async fn initialize() {
        use crate::storage::device::test::*;
        use crate::storage::device::DeviceType;
        simple_logger::init_utc().unwrap();

        let blk = TestManager {
            devices: vec![TestDevice {
                path: PathBuf::from("/dev/test1"),
                device_type: DeviceType::SSD,
                filesystem: None,
                label: None,
                size: 1 * crate::TERABYTE,
            }],
        };

        let mgr = Manager::new(blk, TestPoolManager)
            .await
            .expect("manager failed to create");

        assert_eq!(mgr.ssds.len(), 1);
        assert_eq!(mgr.ssd_size, 1 * crate::TERABYTE);
    }
}
