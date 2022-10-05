use super::StorageManager;
use crate::storage::device::{Device, DeviceManager};
use crate::storage::{pool::*, Manager};
use crate::storage::{Error as StorageError, Kind};
use crate::Unit;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
struct TestUpPool {
    pub name: String,
    pub path: PathBuf,
    pub size: Unit,
    pub volumes: Arc<Mutex<Vec<TestVolume>>>,
}

#[derive(Clone)]
struct TestDownPool {
    pub name: String,
    pub up: TestUpPool,
    pub size: Unit,
}

#[derive(Clone, Default)]
struct TestVolume {
    pub id: u64,
    pub path: PathBuf,
    pub name: String,
    pub usage: Unit,
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
    async fn limit(&self, _size: Option<Unit>) -> Result<()> {
        Ok(())
    }

    async fn usage(&self) -> Result<Unit> {
        Ok(self.usage)
    }
}

#[async_trait::async_trait]
impl DownPool for TestDownPool {
    type UpPool = TestUpPool;

    fn name(&self) -> &str {
        &self.name
    }

    fn size(&self) -> Unit {
        self.size
    }

    async fn up(self) -> std::result::Result<Self::UpPool, UpError<Self>> {
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

    fn size(&self) -> Unit {
        self.size
    }

    /// usage of the pool
    async fn usage(&self) -> Result<Usage> {
        let mut used = 0;
        let vols = self.volumes.lock().await;
        for vol in vols.iter() {
            used += vol.usage().await?;
        }

        Ok(Usage {
            size: self.size,
            used: used,
        })
    }

    /// down bring the pool down and return a DownPool
    async fn down(self) -> std::result::Result<Self::DownPool, DownError<Self>> {
        Ok(TestDownPool {
            name: self.name.clone(),
            up: self,
            size: 1 * crate::TERABYTE,
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

    async fn volume<S: AsRef<str> + Send + Sync>(&self, name: S) -> Result<Self::Volume> {
        match self
            .volumes()
            .await?
            .into_iter()
            .filter(|v| v.name() == name.as_ref())
            .next()
        {
            Some(vol) => Ok(vol),
            None => Err(Error::VolumeNotFound {
                volume: name.as_ref().into(),
            }),
        }
    }
    /// list all volumes in the pool
    async fn volumes(&self) -> Result<Vec<Self::Volume>> {
        let v = self.volumes.lock().await;
        Ok(v.clone())
    }

    /// delete volume pools
    async fn volume_delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()> {
        let mut vols = self.volumes.lock().await;
        vols.retain(|v| v.name() != name.as_ref());
        Ok(())
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
    async fn get(&self, _manager: &M, device: M::Device) -> Result<Pool<TestUpPool, TestDownPool>> {
        //this should use the label, not the path.
        let pool = self.map.get(device.path()).unwrap();

        Ok(pool.clone())
    }
}

#[tokio::test]
async fn manager_initialize_basic() {
    use crate::storage::device::test::*;
    use crate::storage::device::DeviceType;

    let p1_dev: PathBuf = "/dev/test1".into();
    let p1_label: String = "pool-1".into();

    let p2_dev: PathBuf = "/dev/test2".into();
    let p2_label: String = "pool-2".into();

    let p3_dev: PathBuf = "/dev/test3".into();
    let p3_label: String = "pool-3".into();

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
            TestDevice {
                path: p3_dev.clone(),
                device_type: DeviceType::HDD,
                filesystem: Some("test".into()),
                label: Some(p3_label.clone()),
                size: 4 * crate::TERABYTE,
            },
        ],
    };

    // map devices to pools
    let mut pool_manager = TestPoolManager::default();
    pool_manager.map.insert(
        p1_dev.clone(),
        Pool::Down(TestDownPool {
            name: p1_label.clone(),
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p1_label.clone(),
                path: Path::new("/mnt").join(p1_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::default(),
            },
        }),
    );

    pool_manager.map.insert(
        p2_dev.clone(),
        Pool::Down(TestDownPool {
            name: p2_label.clone(),
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p2_label.clone(),
                path: Path::new("/mnt").join(&p2_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::new(Mutex::new(vec![TestVolume {
                    id: 0,
                    name: "zos-cache".into(),
                    path: Path::new("/mnt").join(p2_label).join("zos-cache"),
                    usage: 100 * crate::GIGABYTE,
                }])),
            },
        }),
    );

    pool_manager.map.insert(
        p3_dev.clone(),
        Pool::Down(TestDownPool {
            name: p3_label.clone(),
            size: 4 * crate::TERABYTE,
            up: TestUpPool {
                name: p3_label.clone(),
                path: Path::new("/mnt").join(p3_label),
                size: 4 * crate::TERABYTE,
                volumes: Arc::default(),
            },
        }),
    );

    let mgr = StorageManager::new(blk, pool_manager)
        .await
        .expect("manager failed to create");

    assert_eq!(mgr.ssds.len(), 2);
    assert_eq!(mgr.hdds.len(), 1);
    assert_eq!(mgr.ssd_size, 2 * crate::TERABYTE);
    assert_eq!(mgr.hdd_size, 4 * crate::TERABYTE);

    let pool_1 = &mgr
        .ssds
        .iter()
        .filter(|p| p.name() == "pool-1")
        .next()
        .unwrap();
    assert_eq!(pool_1.state(), State::Down);

    let pool_2 = &mgr
        .ssds
        .iter()
        .filter(|p| p.name() == "pool-2")
        .next()
        .unwrap();
    assert_eq!(pool_2.state(), State::Up);

    let volumes = mgr.volumes().await.unwrap();
    assert_eq!(volumes.len(), 1);

    let cache_vol = &volumes[0];
    assert_eq!(cache_vol.name, "zos-cache");
    assert_eq!(cache_vol.path, Path::new("/mnt/pool-2/zos-cache"));

    // find volume by name.
    let vol = mgr.volume_lookup("zos-cache").await.unwrap();
    assert_eq!(vol.name, "zos-cache");
    assert_eq!(vol.path, Path::new("/mnt/pool-2/zos-cache"));

    let errored = mgr.volume_lookup("not-found").await;

    assert!(matches!(errored, Err(StorageError::NotFound { kind, .. }) if kind == Kind::Volume));
}

#[tokio::test]
async fn manager_vol_create_space_available() {
    // there are 2 pools, one of them is up (because the pool has volumes)
    // on allocation, the up pool is used because it already has enough space

    use crate::storage::device::test::*;
    use crate::storage::device::DeviceType;

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
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p1_label.clone(),
                path: Path::new("/mnt").join(p1_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::default(),
            },
        }),
    );

    pool_manager.map.insert(
        p2_dev.clone(),
        Pool::Down(TestDownPool {
            name: p2_label.clone(),
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p2_label.clone(),
                path: Path::new("/mnt").join(&p2_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::new(Mutex::new(vec![TestVolume {
                    id: 0,
                    name: "zos-cache".into(),
                    path: Path::new("/mnt").join(p2_label).join("zos-cache"),
                    usage: 100 * crate::GIGABYTE,
                }])),
            },
        }),
    );

    let mut mgr = StorageManager::new(blk, pool_manager)
        .await
        .expect("manager failed to create");

    assert_eq!(mgr.ssds.len(), 2);
    assert_eq!(mgr.ssd_size, 2 * crate::TERABYTE);

    let vol = mgr
        .volume_create("vdisks", 20 * crate::GIGABYTE)
        .await
        .unwrap();
    assert_eq!(vol.name, "vdisks");
    assert_eq!(vol.path, Path::new("/mnt/pool-2/vdisks"));
}

#[tokio::test]
async fn manager_vol_create_space_unavailable() {
    // there are 2 pools, one of them is up (because the pool has volumes)
    // on allocation but it's full, the other down pool is brought up instead

    use crate::storage::device::test::*;
    use crate::storage::device::DeviceType;

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
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p1_label.clone(),
                path: Path::new("/mnt").join(&p1_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::default(),
            },
        }),
    );

    pool_manager.map.insert(
        p2_dev.clone(),
        Pool::Down(TestDownPool {
            name: p2_label.clone(),
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p2_label.clone(),
                path: Path::new("/mnt").join(&p2_label),
                size: 1 * crate::TERABYTE,
                volumes: Arc::new(Mutex::new(vec![TestVolume {
                    id: 0,
                    name: "zos-cache".into(),
                    path: Path::new("/mnt").join(&p2_label).join("zos-cache"),
                    usage: 1 * crate::TERABYTE,
                }])),
            },
        }),
    );

    let mut mgr = StorageManager::new(blk, pool_manager)
        .await
        .expect("manager failed to create");

    assert_eq!(
        mgr.ssds
            .iter()
            .filter(|p| p.name() == &p1_label)
            .next()
            .unwrap()
            .state(),
        State::Down
    );

    assert_eq!(
        mgr.ssds
            .iter()
            .filter(|p| p.name() == &p2_label)
            .next()
            .unwrap()
            .state(),
        State::Up
    );

    assert_eq!(mgr.ssds.len(), 2);
    assert_eq!(mgr.ssd_size, 2 * crate::TERABYTE);

    let vol = mgr
        .volume_create("vdisks", 20 * crate::GIGABYTE)
        .await
        .unwrap();
    assert_eq!(vol.name, "vdisks");
    assert_eq!(vol.path, Path::new("/mnt/pool-1/vdisks"));

    let volumes = mgr.volumes().await.unwrap();
    assert_eq!(volumes.len(), 2);

    assert_eq!(
        mgr.ssds
            .iter()
            .filter(|p| p.name() == &p1_label)
            .next()
            .unwrap()
            .state(),
        State::Up
    );

    assert_eq!(
        mgr.ssds
            .iter()
            .filter(|p| p.name() == &p2_label)
            .next()
            .unwrap()
            .state(),
        State::Up
    );
}

#[tokio::test]
async fn manager_vol_delete() {
    use crate::storage::device::test::*;
    use crate::storage::device::DeviceType;

    let p1_dev: PathBuf = "/dev/test1".into();
    let p1_label: String = "pool-1".into();

    let blk = TestManager {
        devices: vec![TestDevice {
            path: p1_dev.clone(),
            device_type: DeviceType::SSD,
            filesystem: Some("test".into()),
            label: Some(p1_label.clone()),
            size: 1 * crate::TERABYTE,
        }],
    };

    // map devices to pools
    let mut pool_manager = TestPoolManager::default();
    pool_manager.map.insert(
        p1_dev.clone(),
        Pool::Down(TestDownPool {
            name: p1_label.clone(),
            size: 1 * crate::TERABYTE,
            up: TestUpPool {
                name: p1_label.clone(),
                path: Path::new("/mnt").join(p1_label.clone()),
                size: 1 * crate::TERABYTE,
                volumes: Arc::new(Mutex::new(vec![TestVolume {
                    id: 0,
                    name: "zos-cache".into(),
                    path: Path::new("/mnt").join(p1_label).join("zos-cache"),
                    usage: 100 * crate::GIGABYTE,
                }])),
            },
        }),
    );

    let mgr = StorageManager::new(blk, pool_manager)
        .await
        .expect("manager failed to create");

    assert_eq!(mgr.ssds.len(), 1);
    assert_eq!(mgr.ssd_size, 1 * crate::TERABYTE);

    let pool_1 = &mgr
        .ssds
        .iter()
        .filter(|p| p.name() == "pool-1")
        .next()
        .unwrap();
    assert_eq!(pool_1.state(), State::Up);

    // find volume by name.
    mgr.volume_delete("zos-cache").await.unwrap();

    let errored = mgr.volume_lookup("zos-cache").await;

    assert!(matches!(errored, Err(StorageError::NotFound { kind, .. }) if kind == Kind::Volume));
}

#[tokio::test]
async fn mkdisk() {
    let path = Path::new("/tmp/disk");
    let result = super::mkdisk(path, 500 * crate::MEGABYTE).await;
    assert!(result.is_ok());
    let meta = tokio::fs::metadata(path).await.unwrap();
    assert_eq!(meta.len(), 500 * crate::MEGABYTE);
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn manager_disk() {
    use crate::storage::device::test::*;
    use crate::storage::device::DeviceType;

    let p1_dev: PathBuf = "/dev/test1".into();
    let p1_label: String = "pool-1".into();

    let blk = TestManager {
        devices: vec![TestDevice {
            path: p1_dev.clone(),
            device_type: DeviceType::SSD,
            filesystem: Some("test".into()),
            label: Some(p1_label.clone()),
            size: 1 * crate::TERABYTE,
        }],
    };

    // map devices to pools
    let mut pool_manager = TestPoolManager::default();
    let pool_path = Path::new("/tmp").join(&p1_label);

    pool_manager.map.insert(
        p1_dev.clone(),
        Pool::Down(TestDownPool {
            name: p1_label.clone(),
            size: 100 * crate::MEGABYTE,
            up: TestUpPool {
                name: p1_label.clone(),
                path: pool_path.clone(),
                size: 1 * crate::TERABYTE,
                volumes: Arc::new(Mutex::new(vec![])),
            },
        }),
    );

    let mut mgr = StorageManager::new(blk, pool_manager)
        .await
        .expect("manager failed to create");

    assert_eq!(mgr.ssds.len(), 1);
    assert_eq!(mgr.ssd_size, 1 * crate::TERABYTE);

    // we know that this will create a volume vdisks but there is no actual
    // call to create the directory in the test scenario so we can do it ahead
    let _ = tokio::fs::remove_dir_all(pool_path.join(super::VDISKS_VOLUME)).await;

    tokio::fs::create_dir_all(pool_path.join(super::VDISKS_VOLUME))
        .await
        .unwrap();

    let disks = mgr.disks().await.unwrap();
    assert_eq!(disks.len(), 0);

    let disk = mgr
        .disk_create("test.50", 50 * crate::MEGABYTE)
        .await
        .unwrap();

    assert_eq!(disk.path, Path::new("/tmp/pool-1/vdisks/test.50"));
    assert_eq!(disk.size, 50 * crate::MEGABYTE);

    let vol = mgr.volume_lookup(super::VDISKS_VOLUME).await.unwrap();
    assert_eq!(vol.path, Path::new("/tmp/pool-1/vdisks"));

    let disk = mgr
        .disk_create("test.25", 25 * crate::MEGABYTE)
        .await
        .unwrap();
    assert_eq!(disk.path, Path::new("/tmp/pool-1/vdisks/test.25"));
    assert_eq!(disk.size, 25 * crate::MEGABYTE);

    let disks = mgr.disks().await.unwrap();
    assert_eq!(disks.len(), 2);

    let disk = disks
        .iter()
        .filter(|d| d.path.file_name().unwrap() == "test.25")
        .next()
        .unwrap();

    assert_eq!(disk.size, 25 * crate::MEGABYTE);
    assert_eq!(disk.path, Path::new("/tmp/pool-1/vdisks/test.25"));

    let disk = mgr.disk_lookup("test.50").await.unwrap();

    assert_eq!(disk.path, Path::new("/tmp/pool-1/vdisks/test.50"));
    assert_eq!(disk.size, 50 * crate::MEGABYTE);

    mgr.disk_delete("test.50").await.unwrap();

    let disks = mgr.disks().await.unwrap();
    assert_eq!(disks.len(), 1);

    let disk = mgr.disk_lookup("test.50").await;
    assert!(matches!(disk, Err(crate::storage::Error::NotFound { .. })));
}
