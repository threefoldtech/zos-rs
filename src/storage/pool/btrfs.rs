use super::{DownPool, Error, InvalidDevice, Pool, PoolManager, Result, UpPool, Usage, Volume};
use crate::storage::device::{Device, DeviceManager, Filesystem};
use crate::system::{Command, Executor, Syscalls};
use crate::Unit;
use anyhow::Context;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// root mount path
const MNT: &str = "/mnt";

/// dir size will calculate the total size of a directory including sub directories
pub async fn dir_size<P: Into<PathBuf>>(root: P) -> std::result::Result<Unit, std::io::Error> {
    use tokio::fs::read_dir;
    let mut paths: Vec<PathBuf> = vec![root.into()];
    let mut index = 0;
    let mut size: Unit = 0;
    while index < paths.len() {
        let path = &paths[index];
        let mut entries = read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            let typ = meta.file_type();
            if typ.is_dir() {
                paths.push(entry.path());
            } else if typ.is_file() {
                size += meta.len();
            }
        }
        index += 1;
    }
    Ok(size)
}

pub struct BtrfsVolume<E>
where
    E: Executor,
{
    utils: Arc<BtrfsUtils<E>>,
    id: u64,
    path: PathBuf,
}

impl<E> BtrfsVolume<E>
where
    E: Executor,
{
    fn new(utils: Arc<BtrfsUtils<E>>, id: u64, path: PathBuf) -> Self {
        Self { utils, id, path }
    }
}

#[async_trait::async_trait]
impl<E> Volume for BtrfsVolume<E>
where
    E: Executor + Send + Sync + 'static,
{
    fn id(&self) -> u64 {
        self.id
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

    async fn limit(&self, size: Option<Unit>) -> Result<()> {
        self.utils.qgroup_limit(&self.path, size).await
    }

    async fn usage(&self) -> Result<Usage> {
        let qgroup = self
            .utils
            .qgroup_list(&self.path)
            .await?
            .into_iter()
            .filter(|g| g.id == format!("0/{}", self.id))
            .next();

        let qgroup = qgroup.ok_or_else(|| Error::QGroupNotFound {
            volume: self.path.clone(),
        })?;

        let used = match qgroup.max_rfer {
            Some(used) => used,
            None => dir_size(&self.path)
                .await
                .context("failed to calculate volume size")?, //TODO: scan all files sizes
        };

        Ok(Usage {
            used: used,
            size: qgroup.max_rfer.unwrap_or(0),
        })
    }
}

pub struct BtrfsDownPool<E, S, D>
where
    E: Executor + 'static,
    S: Syscalls,
    D: Device,
{
    sys: S,
    utils: Arc<BtrfsUtils<E>>,
    device: D,
}

impl<E, S, D> BtrfsDownPool<E, S, D>
where
    E: Executor + Send + Sync + 'static,
    S: Syscalls + Send + Sync,
    D: Device + Send + Sync,
{
    fn new(utils: Arc<BtrfsUtils<E>>, sys: S, device: D) -> Self {
        Self { utils, sys, device }
    }
}

#[async_trait::async_trait]
impl<E, S, D> DownPool for BtrfsDownPool<E, S, D>
where
    E: Executor + Send + Sync + 'static,
    S: Syscalls + Send + Sync,
    D: Device + Send + Sync,
{
    type UpPool = BtrfsUpPool<E, S, D>;

    fn name(&self) -> &str {
        // if we are at this state so device MUST have a label so it's safe to do this
        &self.device.label().unwrap()
    }

    async fn up(mut self) -> Result<Self::UpPool> {
        // mount the device and return the proper UpPool
        let path =
            Path::new(MNT).join(self.device.label().ok_or_else(|| Error::InvalidDevice {
                device: self.device.path().into(),
                reason: InvalidDevice::InvalidLabel,
            })?);

        self.sys.mount(
            Some(self.device.path()),
            &path,
            Option::<&str>::None,
            nix::mount::MsFlags::empty(),
            Option::<&str>::None,
        )?;

        self.utils.qgroup_enable(&path).await?;
        Ok(BtrfsUpPool::new(self.utils, self.sys, path, self.device))
    }
}

pub struct BtrfsUpPool<E, S, D>
where
    E: Executor + 'static,
    S: Syscalls,
    D: Device,
{
    utils: Arc<BtrfsUtils<E>>,
    sys: S,
    device: D,
    path: PathBuf,
}

impl<E, S, D> BtrfsUpPool<E, S, D>
where
    E: Executor + Send + Sync + 'static,
    S: Syscalls + Send + Sync,
    D: Device + Send + Sync,
{
    fn new(utils: Arc<BtrfsUtils<E>>, sys: S, path: PathBuf, device: D) -> Self {
        Self {
            utils,
            sys,
            device,
            path,
        }
    }
}

#[async_trait::async_trait]
impl<E, S, D> UpPool for BtrfsUpPool<E, S, D>
where
    E: Executor + Send + Sync + 'static,
    S: Syscalls + Send + Sync,
    D: Device + Send + Sync,
{
    type Volume = BtrfsVolume<E>;
    type DownPool = BtrfsDownPool<E, S, D>;

    fn path(&self) -> &Path {
        &self.path
    }

    fn name(&self) -> &str {
        // if we are at this state so device MUST have a label so it's safe to do this
        &self.device.label().unwrap()
    }

    async fn usage(&self) -> Result<Usage> {
        let mut used: Unit = 0;
        // this is a very bad implementation because each call to
        // volume.usage() will list all qgroups first then find the
        // corresponding volume id.
        // instead this can be improved by listing once both the
        // volumes and the groups, then just match and calculate once.
        // todo!
        for volume in self.volumes().await? {
            let usage = volume.usage().await?;
            used += usage.used;
        }

        Ok(Usage {
            size: self.device.size(),
            used: used,
        })
    }

    async fn down(mut self) -> Result<Self::DownPool> {
        self.sys.umount(&self.path, None)?;
        Ok(BtrfsDownPool::new(self.utils, self.sys, self.device))
    }

    async fn volumes(&self) -> Result<Vec<Self::Volume>> {
        Ok(self
            .utils
            .volume_list(&self.path)
            .await?
            .into_iter()
            .map(|m| {
                BtrfsVolume::new(
                    Arc::clone(&self.utils),
                    m.id,
                    Path::new(&self.path).join(m.name),
                )
            })
            .collect())
    }

    async fn volume_create<N: AsRef<str> + Send>(&self, name: N) -> Result<Self::Volume> {
        let name = name.as_ref();
        let path = self.utils.volume_create(&self.path, name).await?;
        let id = self.utils.volume_id(&self.path, name).await?;
        Ok(BtrfsVolume::new(Arc::clone(&self.utils), id, path))
    }

    async fn volume_delete<N: AsRef<str> + Send>(&self, name: N) -> Result<()> {
        let name = name.as_ref();
        let id = self.utils.volume_id(&self.path, name).await?;
        self.utils.volume_delete(&self.path, name).await?;
        self.utils.qgroup_delete(&self.path, id).await
    }
}

/// shorthand for a btrfs pool
pub type BtrfsPool<E, S, D> = Pool<BtrfsUpPool<E, S, D>, BtrfsDownPool<E, S, D>>;

impl<E, S, D> BtrfsPool<E, S, D>
where
    E: Executor + Send + Sync + 'static,
    S: Syscalls + Send + Sync,
    D: Device + Send + Sync,
{
    /// create a new btrfs pool from device. the device must have a valid
    /// btrfs filesystem.
    async fn with(exec: E, sys: S, device: D) -> Result<Self> {
        let path = device.path().to_str().ok_or_else(|| Error::InvalidDevice {
            device: device.path().into(),
            reason: InvalidDevice::InvalidPath,
        })?;

        // todo!: create btrfs filesystem and also enable quota
        if device.filesystem().is_none() || device.label().is_none() {
            return Err(Error::InvalidFilesystem {
                device: device.path().into(),
            });
        }

        let mnt = crate::storage::mountinfo(path)
            .await?
            .into_iter()
            .filter(|m| matches!(m.option("subvol"), Some(Some(v)) if v == "/"))
            .next();

        let utils = Arc::new(BtrfsUtils::new(exec));
        match mnt {
            Some(mnt) => Ok(BtrfsPool::Up(BtrfsUpPool::new(
                utils, sys, mnt.target, device,
            ))),
            None => Ok(BtrfsPool::Down(BtrfsDownPool::new(utils, sys, device))),
        }
    }
}

pub struct BtrfsManager<E, S>
where
    E: Executor + Clone,
    S: Syscalls + Clone,
{
    exec: E,
    sys: S,
}

impl<E, S> BtrfsManager<E, S>
where
    E: Executor + Clone + Send + Sync + 'static,
    S: Syscalls + Clone + Send + Sync,
{
    pub fn new(exec: E, sys: S) -> Self {
        Self { exec, sys }
    }
}

#[async_trait::async_trait]
impl<E, S, M> PoolManager<M, BtrfsUpPool<E, S, M::Device>, BtrfsDownPool<E, S, M::Device>>
    for BtrfsManager<E, S>
where
    E: Executor + Clone + Send + Sync + 'static,
    S: Syscalls + Clone + Send + Sync,
    M: DeviceManager + Send + Sync + 'static,
{
    async fn get(&self, manager: &M, device: M::Device) -> Result<BtrfsPool<E, S, M::Device>> {
        let device = match device.filesystem() {
            None => manager
                .format(device, Filesystem::Btrfs, false)
                .await
                .context("failed to prepare filesystem")?,
            Some(fs) if fs == "btrfs" => {
                if device.label().is_some() {
                    device
                } else {
                    // has btrfs but no label! that's an unknown state,
                    return Err(Error::InvalidDevice {
                        device: device.path().into(),
                        reason: InvalidDevice::InvalidLabel,
                    });
                }
            }
            _ => {
                return Err(Error::InvalidFilesystem {
                    device: device.path().into(),
                })
            }
        };

        BtrfsPool::with(self.exec.clone(), self.sys.clone(), device).await
    }
}

struct QGroupInfo {
    id: String,
    #[allow(unused)]
    rfer: Unit,
    #[allow(unused)]
    excl: Unit,
    max_rfer: Option<Unit>,
    #[allow(unused)]
    max_excl: Option<Unit>,
}

struct VolumeInfo {
    id: u64,
    name: String,
}

struct BtrfsUtils<E: Executor> {
    exec: E,
}

impl<E: Executor + 'static> BtrfsUtils<E> {
    fn new(exec: E) -> Self {
        Self { exec }
    }

    async fn volume_create<P: AsRef<Path>, S: AsRef<str>>(
        &self,
        root: P,
        name: S,
    ) -> Result<PathBuf> {
        let path = root.as_ref().join(name.as_ref());
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("create")
            .arg(path);

        self.exec.run(&cmd).await?;
        Ok(root.as_ref().join(name.as_ref()))
    }

    async fn volume_delete<P: AsRef<Path>, S: AsRef<str>>(&self, root: P, name: S) -> Result<()> {
        let path = root.as_ref().join(name.as_ref());
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("delete")
            .arg(path);

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn volume_id<P: AsRef<Path>, S: AsRef<str>>(&self, root: P, name: S) -> Result<u64> {
        let path = root.as_ref().join(name.as_ref());
        let cmd = Command::new("btrfs").arg("subvolume").arg("show").arg(path);

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_volume_info(&output)?)
    }

    async fn volume_list<P: AsRef<Path>>(&self, root: P) -> Result<Vec<VolumeInfo>> {
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("list")
            .arg("-o")
            .arg(root.as_ref());

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_volumes(&output)?)
    }

    async fn qgroup_enable<P: AsRef<Path>>(&self, root: P) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("quota")
            .arg("enable")
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn qgroup_limit<P: AsRef<Path>>(&self, volume: P, size: Option<Unit>) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("limit")
            .arg(match size {
                Some(limit) => format!("{}", limit),
                None => "none".into(),
            })
            .arg(volume.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn qgroup_delete<P: AsRef<Path>>(&self, root: P, volume_id: u64) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("destroy")
            .arg(format!("0/{}", volume_id))
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn qgroup_list<P: AsRef<Path>>(&self, root: P) -> Result<Vec<QGroupInfo>> {
        // qgroup show -re --raw .
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("show")
            .arg("-re")
            .arg("--raw")
            .arg(root.as_ref());

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_qgroup(&output)?)
    }

    fn parse_volume_info(&self, data: &[u8]) -> anyhow::Result<u64> {
        //todo: probably better to use regex or just scan
        //the string until the id is found than allocating strings
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines();
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.splitn(2, ":").collect();
            if parts.len() != 2 {
                continue;
            }
            if parts[0].trim() == "Subvolume ID" {
                return Ok(parts[1].trim().parse()?);
            }
        }

        anyhow::bail!("failed to extract subvolume id")
    }

    fn parse_qgroup(&self, data: &[u8]) -> anyhow::Result<Vec<QGroupInfo>> {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines().skip(2);
        let mut groups = vec![];
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 5 {
                continue;
            }
            let group = QGroupInfo {
                id: parts[0].into(),
                rfer: parts[1].parse()?,
                excl: parts[2].parse()?,
                max_rfer: if parts[3] == "none" {
                    None
                } else {
                    Some(parts[3].parse()?)
                },
                max_excl: if parts[4] == "none" {
                    None
                } else {
                    Some(parts[4].parse()?)
                },
            };
            groups.push(group);
        }

        Ok(groups)
    }

    fn parse_volumes(&self, data: &[u8]) -> anyhow::Result<Vec<VolumeInfo>> {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines();
        let mut volumes = vec![];
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 9 {
                continue;
            }
            let group = VolumeInfo {
                id: parts[1].parse()?,
                name: parts[8].into(),
            };
            volumes.push(group);
        }

        Ok(volumes)
    }
}

impl Default for BtrfsUtils<crate::system::System> {
    fn default() -> Self {
        BtrfsUtils::new(crate::system::System)
    }
}

#[cfg(test)]
mod test {
    use super::{BtrfsPool, BtrfsUtils, DownPool, Pool, UpPool, Volume};
    use crate::storage::device::Device;
    use crate::system::{Command, Syscalls};
    use crate::Unit;
    use anyhow::Result;
    use std::path::{Path, PathBuf};

    // mock syscall always succeed
    // should be improved to validate the inputs
    struct MockSyscalls;
    impl Syscalls for MockSyscalls {
        fn mount<S: AsRef<Path>, T: AsRef<Path>, F: AsRef<str>, D: AsRef<str>>(
            &self,
            _source: Option<S>,
            _target: T,
            _fstype: Option<F>,
            _flags: nix::mount::MsFlags,
            _data: Option<D>,
        ) -> Result<(), crate::system::Error> {
            Ok(())
        }

        fn umount<T: AsRef<Path>>(
            &self,
            _target: T,
            _flags: Option<nix::mount::MntFlags>,
        ) -> Result<(), crate::system::Error> {
            Ok(())
        }
    }

    struct MockDevice {
        path: PathBuf,
        size: Unit,
        label: String,
    }

    impl Device for MockDevice {
        fn path(&self) -> &Path {
            &self.path
        }

        fn size(&self) -> Unit {
            self.size
        }

        fn subsystems(&self) -> &str {
            "mock:device"
        }

        fn filesystem(&self) -> Option<&str> {
            Some("btrfs")
        }

        fn label(&self) -> Option<&str> {
            Some(&self.label)
        }

        fn rota(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn pool_new() {
        const VOLS: &str = r#"ID 256 gen 33152047 top level 5 path zos-cache"#;
        const GROUPS: &str = r#"qgroupid         rfer         excl     max_rfer     max_excl
--------         ----         ----     --------     --------
0/256      1732771840   1732771840 107374182400         none
"#;

        let device = MockDevice {
            path: "/dev/mock".into(),
            size: 100 * crate::GIGABYTE,
            label: "test-device".into(),
        };

        let mut exec = crate::system::MockExecutor::default();
        let list = Command::new("btrfs")
            .arg("subvolume")
            .arg("list")
            .arg("-o")
            .arg("/mnt/test-device");

        let groups = Command::new("btrfs")
            .arg("qgroup")
            .arg("show")
            .arg("-re")
            .arg("--raw")
            .arg("/mnt/test-device/zos-cache");

        let quota = Command::new("btrfs")
            .arg("quota")
            .arg("enable")
            .arg("/mnt/test-device");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &list)
            .returning(|_| Ok(Vec::from(VOLS)));

        exec.expect_run()
            .withf(move |arg: &Command| arg == &groups)
            .returning(|_| Ok(Vec::from(GROUPS)));

        exec.expect_run()
            .withf(move |arg: &Command| arg == &quota)
            .returning(|_| Ok(Vec::default()));

        let pool = BtrfsPool::with(exec, MockSyscalls, device).await.unwrap();
        // because device is NOT (and will never be) mounted. it means pool returned in the mock is always in Down state
        let pool = match pool {
            Pool::Down(pool) => pool,
            _ => panic!("invalid pool type returned"),
        };

        let up = pool.up().await.unwrap();

        assert_eq!(up.name(), "test-device");
        assert_eq!(up.path(), Path::new("/mnt/test-device"));

        let volumes = up.volumes().await.unwrap();
        assert_eq!(volumes.len(), 1);
        let cache = &volumes[0];

        assert_eq!(cache.id(), 256);
        assert_eq!(cache.path(), Path::new("/mnt/test-device/zos-cache"));

        let usage = cache.usage().await.unwrap();
        assert_eq!(usage.size, 100 * crate::GIGABYTE);
        assert_eq!(usage.used, 100 * crate::GIGABYTE);
    }

    #[test]
    fn utils_vol_info_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"b623b3b159fa02652bb21c695a157b4d
        Name: 			b623b3b159fa02652bb21c695a157b4d
        UUID: 			abf4240e-6402-9947-963e-63db1a7f5582
        Parent UUID: 		-
        Received UUID: 		-
        Creation time: 		2022-02-03 12:58:32 +0000
        Subvolume ID: 		1740
        Generation: 		33008608
        Gen at creation: 	199304
        Parent ID: 		5
        Top level ID: 		5
        Flags: 			-
        Snapshot(s):
        "#;

        let id = utils.parse_volume_info(DATA.as_bytes()).unwrap();
        assert_eq!(id, 1740);
    }

    #[test]
    fn utils_qgroup_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"qgroupid         rfer         excl     max_rfer     max_excl
--------         ----         ----     --------     --------
0/256      1732771840   1732771840 107374182400         none
0/262     60463501312  60463501312         none         none
0/1596          16384        16384     10485760         none
0/1737          16384        16384     10485760         none
0/1740          16384        16384     10485760         none
0/4301      524271616    524271616    524288000         none
0/4303      524271616    524271616    524288000         none
0/4849      106655744    106655744   2147483648         none
0/7437        6471680      6471680  10737418240         none
0/7438     1525182464   1525182464   2147483648         none
        "#;

        let groups = utils.parse_qgroup(DATA.as_bytes()).unwrap();
        assert_eq!(groups.len(), 10);
        let group0 = &groups[0];
        let group1 = &groups[1];

        assert_eq!(group0.id, "0/256");
        assert_eq!(group0.rfer, 1732771840);
        assert_eq!(group0.excl, 1732771840);
        assert_eq!(group0.max_rfer, Some(107374182400));
        assert_eq!(group0.max_excl, None);

        assert_eq!(group1.id, "0/262");
        assert_eq!(group1.rfer, 60463501312);
        assert_eq!(group1.excl, 60463501312);
        assert_eq!(group1.max_rfer, None);
        assert_eq!(group1.max_excl, None);
    }

    #[test]
    fn utils_volumes_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"ID 256 gen 33152047 top level 5 path zos-cache
ID 262 gen 33152049 top level 5 path vdisks
ID 1596 gen 117776 top level 5 path bfb95cf4f1b6245f56a7fb7a86bd1e0d
ID 1737 gen 156823 top level 5 path 794e0004fd49a7300d612dcbba10279f
ID 1740 gen 33008608 top level 5 path b623b3b159fa02652bb21c695a157b4d
ID 4301 gen 5392957 top level 5 path rootfs:433-3764-mr
ID 4303 gen 32919873 top level 5 path rootfs:433-3764-w1
ID 4849 gen 33152049 top level 5 path rootfs:288-5475-owncloud_samehabouelsaad
ID 7437 gen 33152049 top level 5 path 647-10988-qsfs
ID 7438 gen 33152049 top level 5 path rootfs:647-10988-vm
        "#;

        let vols = utils.parse_volumes(DATA.as_bytes()).unwrap();
        assert_eq!(vols.len(), 10);
        let vol0 = &vols[0];
        let vol1 = &vols[1];

        assert_eq!(vol0.id, 256);
        assert_eq!(vol0.name, "zos-cache");

        assert_eq!(vol1.id, 262);
        assert_eq!(vol1.name, "vdisks");
    }

    #[tokio::test]
    async fn utils_volume_create() {
        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("create")
            .arg("/mnt/pool/test");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));

        let vol = utils.volume_create("/mnt/pool", "test").await.unwrap();
        utils.exec.checkpoint();
        assert_eq!(vol, Path::new("/mnt/pool/test"))
    }

    #[tokio::test]
    async fn utils_volume_delete() {
        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("delete")
            .arg("/mnt/pool/test");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));

        utils.volume_delete("/mnt/pool", "test").await.unwrap();
        utils.exec.checkpoint();
    }

    #[tokio::test]
    async fn utils_volume_id() {
        const DATA: &str = r#"b623b3b159fa02652bb21c695a157b4d
        Name: 			b623b3b159fa02652bb21c695a157b4d
        UUID: 			abf4240e-6402-9947-963e-63db1a7f5582
        Parent UUID: 		-
        Received UUID: 		-
        Creation time: 		2022-02-03 12:58:32 +0000
        Subvolume ID: 		1740
        Generation: 		33008608
        Gen at creation: 	199304
        Parent ID: 		5
        Top level ID: 		5
        Flags: 			-
        Snapshot(s):
        "#;

        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("show")
            .arg("/mnt/pool/test");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::from(DATA)));

        let vol = utils.volume_id("/mnt/pool", "test").await.unwrap();
        utils.exec.checkpoint();
        assert_eq!(vol, 1740);
    }

    #[tokio::test]
    async fn utils_volume_list() {
        const DATA: &str = r#"ID 256 gen 33152047 top level 5 path zos-cache
ID 262 gen 33152049 top level 5 path vdisks
ID 1596 gen 117776 top level 5 path bfb95cf4f1b6245f56a7fb7a86bd1e0d
ID 1737 gen 156823 top level 5 path 794e0004fd49a7300d612dcbba10279f
ID 1740 gen 33008608 top level 5 path b623b3b159fa02652bb21c695a157b4d
ID 4301 gen 5392957 top level 5 path rootfs:433-3764-mr
ID 4303 gen 32919873 top level 5 path rootfs:433-3764-w1
ID 4849 gen 33152049 top level 5 path rootfs:288-5475-owncloud_samehabouelsaad
ID 7437 gen 33152049 top level 5 path 647-10988-qsfs
ID 7438 gen 33152049 top level 5 path rootfs:647-10988-vm
        "#;

        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("list")
            .arg("-o")
            .arg("/mnt/pool");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::from(DATA)));

        let vols = utils.volume_list("/mnt/pool").await.unwrap();
        utils.exec.checkpoint();
        assert_eq!(vols.len(), 10);
        let vol0 = &vols[0];
        let vol1 = &vols[1];

        assert_eq!(vol0.id, 256);
        assert_eq!(vol0.name, "zos-cache");

        assert_eq!(vol1.id, 262);
        assert_eq!(vol1.name, "vdisks");
    }

    #[tokio::test]
    async fn utils_qgroup_enable() {
        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("quota")
            .arg("enable")
            .arg("/mnt/pool");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));

        utils.qgroup_enable("/mnt/pool").await.unwrap();
        utils.exec.checkpoint();
    }

    #[tokio::test]
    async fn utils_qgroup_destroy() {
        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("destroy")
            .arg(format!("0/{}", 250))
            .arg("/mnt/pool");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));

        utils.qgroup_delete("/mnt/pool", 250).await.unwrap();
        utils.exec.checkpoint();
    }

    #[tokio::test]
    async fn utils_qgroup_list() {
        const DATA: &str = r#"qgroupid         rfer         excl     max_rfer     max_excl
--------         ----         ----     --------     --------
0/256      1732771840   1732771840 107374182400         none
0/262     60463501312  60463501312         none         none
0/1596          16384        16384     10485760         none
0/1737          16384        16384     10485760         none
0/1740          16384        16384     10485760         none
0/4301      524271616    524271616    524288000         none
0/4303      524271616    524271616    524288000         none
0/4849      106655744    106655744   2147483648         none
0/7437        6471680      6471680  10737418240         none
0/7438     1525182464   1525182464   2147483648         none
        "#;

        let exec = crate::system::MockExecutor::default();
        let mut utils = BtrfsUtils::new(exec);
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("show")
            .arg("-re")
            .arg("--raw")
            .arg("/mnt/pool");
        utils
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::from(DATA)));

        let groups = utils.qgroup_list("/mnt/pool").await.unwrap();
        utils.exec.checkpoint();

        assert_eq!(groups.len(), 10);
        let group0 = &groups[0];
        let group1 = &groups[1];

        assert_eq!(group0.id, "0/256");
        assert_eq!(group0.rfer, 1732771840);
        assert_eq!(group0.excl, 1732771840);
        assert_eq!(group0.max_rfer, Some(107374182400));
        assert_eq!(group0.max_excl, None);

        assert_eq!(group1.id, "0/262");
        assert_eq!(group1.rfer, 60463501312);
        assert_eq!(group1.excl, 60463501312);
        assert_eq!(group1.max_rfer, None);
        assert_eq!(group1.max_excl, None);
    }
}
