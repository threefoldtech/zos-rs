use super::db::MetadataDbMgr;
use super::volume_allocator::VolumeAllocator;
use crate::bus::types::storage::{MountMode, MountOptions, WriteLayer};
use crate::env;
use crate::storage::{self, G8ufsInfo};
use crate::system::{Command, Executor, Syscalls};
use anyhow::{bail, Result};
use async_recursion::async_recursion;
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
        let log_path = Path::new(&self.log).join(&log_name);

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

    pub async fn mount_overlay<T: AsRef<str>, B: AsRef<Path>, C: AsRef<Path>>(
        &self,
        name: T,
        ro: B,
        mountpoint: C,
        opts: &MountOptions,
    ) -> Result<()> where {
        fs::create_dir_all(&mountpoint).await?;
        if let MountMode::ReadWrite(WriteLayer::Size(limit)) = opts.mode {
            // no persisted volume provided, hence
            // we need to create one, or find one that is already exists
            let persistent = match self.storage.lookup(&name) {
                Ok(volume) => volume.path,
                Err(_) => {
                    // Volume doesn't exist create a new one
                    if limit == 0 {
                        bail!("invalid mount option, missing disk type");
                    }
                    match self.storage.create(&name, limit) {
                        Ok(volume) => volume.path,
                        Err(e) => {
                            self.storage.delete(&name)?;
                            bail!(e)
                        }
                    }
                }
            };
            let rw = persistent.join("rw");
            let wd = persistent.join("wd");
            let paths = vec![&rw, &wd];
            for path in paths {
                fs::create_dir_all(&path).await?;
            }
            let data = format!(
                "lowerdir={},upperdir={},workdir={}",
                ro.as_ref().display(),
                &rw.display(),
                &wd.display()
            );
            self.syscalls.mount(
                Some("overlay"),
                mountpoint,
                Some("overlay"),
                nix::mount::MsFlags::MS_NOATIME,
                Some(&data),
            )?;
        };
        Ok(())
    }

    pub async fn clean_unused_mounts(&self) -> Result<()> {
        let all = storage::mounts().await?;
        let mut ro_targets = HashMap::new();
        // Get all flists managed by flist Daemony
        let ros = all
            .clone()
            .into_iter()
            .filter(|mnt_info| Path::new(&mnt_info.target).starts_with(&self.root))
            .filter(|mnt_info| {
                Path::new(&mnt_info.target).parent() == Some(&self.ro)
                    && mnt_info.filesystem == FsType::G8UFS.as_ref()
            });

        for mount in ros {
            let g8ufs = mount.as_g8ufs()?;
            ro_targets.insert(g8ufs.pid, mount);
        }

        let all_under_mountpoints = all
            .clone()
            .into_iter()
            .filter(|mount| Path::new(&mount.target).parent() == Some(&self.mountpoint));

        for mount in all_under_mountpoints {
            let pid: i64;
            if mount.filesystem == FsType::G8UFS.as_ref() {
                pid = mount.as_g8ufs()?.pid
            } else if mount.filesystem == FsType::Overlay.as_ref() {
                let lower_dir_path = mount.as_overlay()?.lower_dir;
                let mut all_matching_overlay = all
                    .clone()
                    .into_iter()
                    .filter(|mnt| Path::new(&lower_dir_path) == Path::new(&mnt.target));
                pid = match all_matching_overlay.next() {
                    Some(mount) => mount.as_g8ufs()?.pid,
                    None => continue,
                };
            } else {
                continue;
            }
            ro_targets.remove(&pid);
        }
        for (_, mount) in ro_targets.into_iter() {
            log::debug!("cleaning up mount {:#?}", mount);
            if let Err(err) = self.syscalls.umount(&mount.target, None) {
                log::debug!("failed to unmount {:#?} Error: {}", mount, err);
                continue;
            }
            if let Err(err) = fs::remove_dir_all(&mount.target).await {
                log::debug!(
                    "failed to remove dir {:#?} for mount {:#?} Error: {}",
                    mount.target,
                    mount,
                    err
                );
            }
        }
        Ok(())
    }

    #[async_recursion]
    pub async fn resolve<T: AsRef<Path> + Send>(&self, path: T) -> Result<G8ufsInfo> {
        match storage::mountpoint(path).await? {
            Some(mount) => {
                if mount.filesystem == FsType::G8UFS.as_ref() {
                    let g8ufsinfo = mount.as_g8ufs()?;
                    Ok(g8ufsinfo)
                } else if mount.filesystem == FsType::Overlay.as_ref() {
                    let overlay = mount.as_overlay()?;
                    self.resolve(&overlay.lower_dir).await
                } else {
                    bail!("invalid mount fs type {}", mount.filesystem)
                }
            }
            None => bail!("failed to get mount info"),
        }
    }
}
