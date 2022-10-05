use super::db::MetadataDbMgr;
use super::volume_allocator::VolumeAllocator;
use crate::env;
use crate::storage;
use crate::system::{Command, Executor, Syscalls};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::time;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FsType {
    G8UFS,
    Overlay,
    Other,
}
impl AsRef<str> for FsType {
    fn as_ref(&self) -> &str {
        match self {
            FsType::G8UFS => "fuse.g8ufs",
            FsType::Overlay => "overlay",
            FsType::Other => "other",
        }
    }
}
// type MResult<T> = anyhow::Result<T, Error>;
pub struct MountManager<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
    E: Executor,
{
    // root directory where all
    // the working file of the module will be located
    pub root: PathBuf,

    // underneath are the path for each
    // sub folder used by the flist module
    pub flist: PathBuf,
    pub cache: PathBuf,
    pub mountpoint: PathBuf,
    pub ro: PathBuf,
    pub log: PathBuf,
    pub syscalls: S,
    pub storage: A,
    pub executor: E,
    pub db: MetadataDbMgr,
}
impl<A, S, E> MountManager<A, S, E>
where
    A: VolumeAllocator + Sync + Send,
    S: Syscalls + Sync + Send,
    E: Executor + Sync + Send,
{
    pub async fn new<R: Into<PathBuf>>(
        root: R,
        syscalls: S,
        storage: A,
        executor: E,
    ) -> Result<Self>
    where
        R: AsRef<str>,
    {
        let root = root.into();
        let db = MetadataDbMgr::new(root.join("flist")).await?;
        fs::create_dir_all(&root).await?;
        // prepare directory layout for the module
        for path in &["flist", "cache", "mountpoint", "ro", "log"] {
            fs::create_dir_all(&root.join(path)).await?;
        }
        Ok(Self {
            flist: root.join("flist"),
            cache: root.join("cache"),
            mountpoint: root.join("mountpoint"),
            ro: root.join("ro"),
            log: root.join("log"),
            root,
            syscalls,
            storage,
            executor,
            db,
        })
    }

    // returns the mount path out of an flist name simplly joins /<FLISTS_ROOT>/<mountpoint>/<name>
    // this where this flist instance will be
    pub fn mountpath<T: AsRef<str>>(&self, name: T) -> Result<PathBuf> {
        let mountpath = self.mountpoint.join(name.as_ref());
        if mountpath.parent() != Some(self.mountpoint.as_path()) {
            bail!("inavlid mount name: {}", name.as_ref());
        }
        Ok(mountpath)
    }

    // returns ro path joined with flist hash
    // this where we mount the flist for read only
    fn flist_ro_mount_path<R: AsRef<str>>(&self, hash: R) -> Result<PathBuf> {
        let mountpath = self.ro.join(hash.as_ref());
        if mountpath.parent() != Some(self.ro.as_path()) {
            bail!("invalid mount name")
        }

        Ok(mountpath)
    }

    // Checks if the given path is mountpoint or not
    pub async fn is_mounted<P: AsRef<Path>>(&self, path: P) -> bool {
        storage::mountpoint(path.as_ref()).await.is_ok()
    }

    // Checks is the given path is a valid mountpoint means:
    // it is either doesn't exist or
    // it is a dir and not a mountpoint for anything
    pub async fn valid<P: AsRef<Path>>(&self, path: P) -> bool {
        match fs::metadata(&path).await {
            Ok(info) => info.is_dir() && !self.is_mounted(&path).await,
            Err(err) if err.kind() == io::ErrorKind::NotFound => true,
            Err(err) if err.kind() == io::ErrorKind::ConnectionAborted => {
                matches!(self.syscalls.umount(path, None), Ok(_))
            }
            _ => false,
        }
    }

    async fn wait_mountpoint<P: AsRef<Path>>(&self, path: P, seconds: u32) -> Result<()> {
        let mut duration = seconds;
        while duration > 0 {
            time::sleep(time::Duration::from_secs(1)).await;
            if self.is_mounted(path.as_ref()).await {
                return Ok(());
            }
            duration -= 1;
        }

        bail!("was not mounted in time")
    }

    // MountRO mounts an flist in read-only mode. This mount then can be shared between multiple rw mounts
    // TODO: how to know that this ro mount is no longer used, hence can be unmounted and cleaned up?
    // this mounts the downloaded flish under <FLISTS_ROOT>/ro/<FLIST_HASH>
    pub async fn mount_ro<T: AsRef<str>, W: AsRef<str>>(
        &self,
        url: T,
        storage_url: Option<W>,
    ) -> Result<PathBuf> {
        // this should return always the flist mountpoint. which is used
        // as a base for all RW mounts.
        let flist_path = self.db.get(url).await?;

        let hash = match flist_path.file_name() {
            Some(hash) => match hash.to_str() {
                Some(hash) => hash,
                None => bail!("failed to get flist hash"),
            },
            None => bail!("failed to get flist hash"),
        };

        let ro_mountpoint = self.flist_ro_mount_path(&hash)?;
        if self.is_mounted(&ro_mountpoint).await {
            return Ok(ro_mountpoint);
        }
        if !self.valid(&ro_mountpoint).await {
            bail!("invalid mountpoint {}", &ro_mountpoint.display())
        }

        fs::create_dir_all(&ro_mountpoint).await?;
        let storage_url = match storage_url {
            Some(storage_url) => storage_url.as_ref().to_string(),
            None => {
                let environ = env::get()?;
                environ.storage_url
            }
        };

        let log_name = format!("{}.log", hash);
        let log_path = self.log.join(&log_name);

        let cmd = Command::new("g8ufs")
            .arg("--cache")
            .arg(self.cache.as_os_str())
            .arg("--meta")
            .arg(flist_path)
            .arg("--storage-url")
            .arg(storage_url)
            .arg("--daemon")
            .arg("--log")
            .arg(log_path.as_os_str())
            .arg("--ro")
            .arg(&ro_mountpoint.as_os_str());
        self.executor.run(&cmd).await?;
        nix::unistd::sync();
        Ok(ro_mountpoint)
    }

    // Create bind mount for <FLISTS_ROOT>/ro/<FLIST_HASH> on <FLISTS_ROOT>/mountpoint/<name>
    pub async fn mount_bind<P: AsRef<Path>, T: AsRef<Path>>(
        &self,
        ro_mount_path: P,
        mountpoint: T,
    ) -> Result<bool> {
        fs::create_dir_all(&mountpoint).await?;
        if self
            .syscalls
            .mount(
                Some(ro_mount_path),
                &mountpoint,
                Some("bind"),
                nix::mount::MsFlags::MS_BIND,
                Option::<&str>::None,
            )
            .is_err()
        {
            if let Err(err) = self.syscalls.umount(&mountpoint, None) {
                log::debug!(
                    "failed to unmount {}, Error: {}",
                    &mountpoint.as_ref().display(),
                    err
                );
            }
            return Ok(false);
        };
        self.wait_mountpoint(&mountpoint, 3).await?;
        Ok(true)
    }

    pub async fn mount_overlay<B: AsRef<Path>, C: AsRef<Path>, D: AsRef<Path>>(
        &self,
        ro: B,
        rw: C,
        mountpoint: D,
    ) -> Result<()> where {
        fs::create_dir_all(&mountpoint).await?;
        let rw_dir = rw.as_ref().join("rw");
        let work_dir = rw.as_ref().join("wd");
        let paths = vec![&rw_dir, &work_dir];
        for path in paths {
            fs::create_dir_all(&path).await?;
        }
        let data = format!(
            "lowerdir={},upperdir={},workdir={}",
            ro.as_ref().display(),
            &rw_dir.display(),
            &work_dir.display()
        );
        self.syscalls.mount(
            Some("overlay"),
            mountpoint,
            Some("overlay"),
            nix::mount::MsFlags::MS_NOATIME,
            Some(&data),
        )?;
        Ok(())
    }

    pub async fn clean_unused_mounts(&self) -> Result<()> {
        let all = storage::mounts().await?;
        let mut ro_targets = HashMap::new();
        // Get all flists managed by flist Daemon
        let ros = all
            .iter()
            .filter(|mnt_info| mnt_info.target.starts_with(&self.root))
            .filter(|mnt_info| {
                mnt_info.target.parent() == Some(&self.ro)
                    && mnt_info.filesystem == FsType::G8UFS.as_ref()
            });

        for mount in ros {
            let pid: i64 = mount.source.parse()?;
            ro_targets.insert(pid, mount);
        }

        let all_under_mountpoints = all
            .iter()
            .filter(|mount| mount.target.parent() == Some(&self.mountpoint));

        for mount in all_under_mountpoints {
            let pid: i64;
            if mount.filesystem == FsType::G8UFS.as_ref() {
                pid = mount.source.parse()?
            } else if mount.filesystem == FsType::Overlay.as_ref() {
                // let lower_dir_path = mount.as_overlay()?.lower_dir;
                let lower_dir = match mount.option("lowerdir") {
                    Some(Some(lower_dir)) => lower_dir,
                    _ => bail!("bad overlay options: lowerdir not found"),
                };
                let mut all_matching_overlay = all
                    .iter()
                    .filter(|mnt| PathBuf::from(lower_dir) == mnt.target);
                pid = match all_matching_overlay.next() {
                    Some(mount) => mount.source.parse()?,
                    None => continue,
                };
            } else {
                continue;
            }
            ro_targets.remove(&pid);
        }
        for (_, mount) in ro_targets.iter() {
            log::debug!("cleaning up mount {}", &mount.target.display());
            if let Err(err) = self.syscalls.umount(&mount.target, None) {
                log::debug!(
                    "failed to unmount {} Error: {}",
                    mount.target.display(),
                    err
                );
                continue;
            }
            if let Err(err) = fs::remove_dir_all(&mount.target).await {
                log::debug!("failed to remove dir {:#?}  Error: {}", mount.target, err);
            }
        }
        Ok(())
    }

    pub async fn get_volume_path<T: AsRef<str>>(&self, name: T, size: u64) -> Result<PathBuf> {
        // no persisted volume provided, hence
        // we need to create one, or find one that is already exists
        match self.storage.volume_lookup(&name) {
            Ok(volume) => Ok(volume.path),
            Err(_) => {
                // Volume doesn't exist create a new one
                if size == 0 {
                    bail!("invalid mount option, missing disk type");
                }
                match self.storage.volume_create(&name, size) {
                    Ok(volume) => Ok(volume.path),
                    Err(e) => {
                        self.storage.volume_delete(&name)?;
                        bail!(e)
                    }
                }
            }
        }
    }

    pub async fn resolve<T: Into<PathBuf>>(&self, path: T) -> Result<u64> {
        let mut path = path.into();
        loop {
            match storage::mountpoint(&path).await? {
                None => bail!("failed to get mount info of {}", path.display()),
                Some(mnt) if mnt.filesystem == FsType::G8UFS.as_ref() => {
                    return Ok(mnt.source.parse()?)
                }
                Some(mnt) if mnt.filesystem == FsType::Overlay.as_ref() => {
                    if let Some(Some(p)) = mnt.option("lowerdir") {
                        path = PathBuf::from(p)
                    } else {
                        bail!("invalid overlay options: {}", path.display())
                    }
                }
                _ => bail!("unknown filesystem in path: {}", path.display()),
            };
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use super::MountManager;
    use crate::{
        flist::volume_allocator::MockVolumeAllocator,
        system::{Command, Syscalls},
    };
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
    #[tokio::test]
    async fn test_mount_ro() {
        let executor = crate::system::MockExecutor::default();

        let mut mount_mgr =
            MountManager::new("/tmp/flist", MockSyscalls, MockVolumeAllocator, executor)
                .await
                .unwrap();
        let flist_path = mount_mgr.flist.join("efc9269253cb7210d6eded4aa53b7dfc");
        let ro_mountpoint = mount_mgr.ro.join("efc9269253cb7210d6eded4aa53b7dfc");
        let log_path = mount_mgr.log.join("efc9269253cb7210d6eded4aa53b7dfc.log");
        let storage_url = "http://storage-url.com";
        let cmd = Command::new("g8ufs")
            .arg("--cache")
            .arg(mount_mgr.cache.as_os_str())
            .arg("--meta")
            .arg(flist_path)
            .arg("--storage-url")
            .arg("test")
            .arg("--daemon")
            .arg("--log")
            .arg(log_path.as_os_str())
            .arg("--ro")
            .arg(&ro_mountpoint.as_os_str());
        mount_mgr
            .executor
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));

        mount_mgr
            .mount_ro(
                "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist",
                Some(storage_url),
            )
            .await
            .unwrap();
    }
}
