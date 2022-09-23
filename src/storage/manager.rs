use super::pool;
use super::Result;
use super::VolumeInfo;
use crate::cache::Store;
use crate::storage::device::{DeviceManager, DeviceType};
use crate::storage::pool::{DownPool, UpPool, Volume};
use crate::Unit;
use anyhow::Context;
use std::collections::HashMap;

use super::device::Device;
use super::pool::{Pool, PoolManager};

const FORCE_FORMAT: bool = false;
const CACHE_VOLUME: &str = "zos-cache";
const CACHE_TARGET: &str = "/var/cache";

pub struct StorageManager<M, P, U, D>
where
    M: DeviceManager,
    P: PoolManager<M, U, D>,
    U: UpPool,
    D: DownPool,
{
    device_mgr: M,
    pool_mgr: P,
    ssds: HashMap<String, Pool<U, D>>,
    hdds: HashMap<String, Pool<U, D>>,
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

    // async fn ensure_cache(&self) -> Result<()> {
    //     let mnt = super::mountpoint(CACHE_TARGET)
    //         .await
    //         .context("failed to check mount for cache")?;

    //     if mnt.is_some() {
    //         return Ok(());
    //     }

    //     let vol = match self.find_volume(CACHE_VOLUME).await? {
    //         Some(vol) => vol,
    //         None => unimplemented!(), // create a volume
    //     };

    //     System.mount(
    //         Some(vol.path()),
    //         CACHE_TARGET,
    //         Option::<&str>::None,
    //         MsFlags::MS_BIND,
    //         Option::<&str>::None,
    //     )?;

    //     Ok(())
    // }

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

#[async_trait::async_trait]
impl<M, P, U, D> super::Manager for StorageManager<M, P, U, D>
where
    M: DeviceManager,
    P: PoolManager<M, U, D>,
    U: UpPool,
    D: DownPool,
{
    async fn volume_lookup<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<VolumeInfo> {
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
                return Ok((&vol).into());
            }
        }

        Err(super::Error::NotFound {
            id: name.as_ref().into(),
            kind: super::Kind::Volume,
        })
    }

    async fn volumes(&self) -> Result<Vec<VolumeInfo>> {
        let mut volumes = vec![];
        for (_, pool) in self.ssds.iter() {
            let up = match pool {
                Pool::Up(ref up) => up,
                _ => continue,
            };

            volumes.extend(up.volumes().await?.iter().map(|v| VolumeInfo::from(v)));
        }

        Ok(volumes)
    }
}

#[cfg(test)]
mod test {

    use super::StorageManager;
    use crate::storage::device::{Device, DeviceManager};
    use crate::storage::pool::*;
    use crate::Unit;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct TestUpPool {
        pub name: String,
        pub path: PathBuf,
        pub usage: Usage,
        pub volumes: Arc<Mutex<Vec<TestVolume>>>,
    }

    #[derive(Clone)]
    struct TestDownPool {
        pub name: String,
        pub up: TestUpPool,
    }

    #[derive(Clone, Default)]
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
            let name = name.as_ref();
            let mut vols = self.volumes.lock().await;
            for vol in vols.iter() {
                if vol.name() == name {
                    return Err(Error::VolumeAlreadyExists {
                        volume: name.into(),
                    });
                }
            }
            let vol = TestVolume {
                id: (vols.len() + 1) as u64,
                name: name.into(),
                path: self.path.join(name),
                ..Default::default()
            };

            vols.push(vol.clone());
            // other wise just create
            Ok(vol)
        }

        /// list all volumes in the pool
        async fn volumes(&self) -> Result<Vec<Self::Volume>> {
            let v = self.volumes.lock().await;
            Ok(v.clone())
        }

        /// delete volume pools
        async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct TestPoolManager {
        pub map: HashMap<PathBuf, Pool<TestUpPool, TestDownPool>>,
    }

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
            //this should use the label, not the path.
            let pool = self.map.get(device.path()).unwrap();

            Ok(pool.clone())
        }
    }

    #[tokio::test]
    async fn initialize() {
        use crate::storage::device::test::*;
        use crate::storage::device::DeviceType;
        simple_logger::init_utc().unwrap();

        let p1_dev: PathBuf = "/dev/test1".into();
        let p1_label: String = "pool-1".into();

        let p2_dev: PathBuf = "/dev/test2".into();
        let p2_label: String = "pool-2".into();

        let blk = TestManager {
            devices: vec![
                TestDevice {
                    path: p1_dev.clone(),
                    device_type: DeviceType::SSD,
                    filesystem: Some("test".into()),
                    label: Some(p1_label.clone()),
                    size: 1 * crate::TERABYTE,
                },
                TestDevice {
                    path: p2_dev.clone(),
                    device_type: DeviceType::SSD,
                    filesystem: Some("test".into()),
                    label: Some(p2_label.clone()),
                    size: 1 * crate::TERABYTE,
                },
            ],
        };

        // map devices to pools
        let mut pool_manager = TestPoolManager::default();
        pool_manager.map.insert(
            p1_dev.clone(),
            Pool::Down(TestDownPool {
                name: p1_label.clone(),
                up: TestUpPool {
                    name: p1_label.clone(),
                    path: Path::new("/mnt").join(p1_label),
                    usage: Usage {
                        size: 1 * crate::TERABYTE,
                        used: 0,
                    },
                    volumes: Arc::default(),
                },
            }),
        );

        pool_manager.map.insert(
            p2_dev.clone(),
            Pool::Down(TestDownPool {
                name: p2_label.clone(),
                up: TestUpPool {
                    name: p2_label.clone(),
                    path: Path::new("/mnt").join(&p2_label),
                    usage: Usage {
                        size: 1 * crate::TERABYTE,
                        used: 0,
                    },
                    volumes: Arc::new(Mutex::new(vec![TestVolume {
                        id: 0,
                        name: "zos-cache".into(),
                        path: Path::new("/mnt").join(p2_label).join("zos-cache"),
                        usage: Usage {
                            size: 100 * crate::GIGABYTE,
                            used: 100 * crate::GIGABYTE,
                        },
                    }])),
                },
            }),
        );

        let mgr = StorageManager::new(blk, pool_manager)
            .await
            .expect("manager failed to create");

        assert_eq!(mgr.ssds.len(), 2);
        assert_eq!(mgr.ssd_size, 2 * crate::TERABYTE);
        let pool_1 = &mgr.ssds["pool-1"];
        assert_eq!(pool_1.state(), State::Down);

        let pool_2 = &mgr.ssds["pool-2"];
        assert_eq!(pool_2.state(), State::Up);
    }
}
