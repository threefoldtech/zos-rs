mod mgr;
/// implementation of the flist daemon
mod utils;
mod volume_allocator;
use crate::bus::api::Flist;
use crate::bus::types::storage::MountMode;
use crate::bus::types::storage::MountOptions;
use crate::bus::types::storage::WriteLayer;
use crate::env;
use crate::system::Command;
use crate::system::Executor;
use crate::system::Syscalls;

use anyhow::{bail, Result};
use nix;
use nix::mount;
use std::fs;
use std::fs::Permissions;
use std::os::unix::prelude::PermissionsExt;

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use self::utils::mount_bind;
use self::volume_allocator::VolumeAllocator;

pub struct FListDaemon<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
    E: Executor,
{
    // root directory where all
    // the working file of the module will be located
    root: PathBuf,

    // underneath are the path for each
    // sub folder used by the flist module
    flist: PathBuf,
    cache: PathBuf,
    mountpoint: PathBuf,
    ro: PathBuf,
    pid: PathBuf,
    log: PathBuf,
    syscalls: S,
    storage: A,
    executor: E,
}

impl<A, S, E> FListDaemon<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
    E: Executor,
{
    fn new<R: Into<PathBuf>>(root: R, syscalls: S, storage: A, executor: E) -> Self
    where
        R: AsRef<str>,
    {
        let root = Path::new(root.as_ref());
        Self {
            root: root.into(),
            flist: root.join("flist").into(),
            cache: root.join("cache").into(),
            mountpoint: root.join("mountpoint").into(),
            ro: root.join("ro").into(),
            pid: root.join("pid").into(),
            log: root.join("log").into(),
            syscalls,
            storage,
            executor,
        }
    }

    // MountRO mounts an flist in read-only mode. This mount then can be shared between multiple rw mounts
    // TODO: how to know that this ro mount is no longer used, hence can be unmounted and cleaned up?
    async fn mount_ro<T: AsRef<str>, W: AsRef<str>>(
        &self,
        url: T,
        storage_url: Option<W>,
    ) -> Result<PathBuf> {
        // this should return always the flist mountpoint. which is used
        // as a base for all RW mounts.
        let hash = match utils::hash_of_flist(&url).await {
            Ok(hash) => hash,
            Err(_) => bail!("Failed to get flist hash"),
        };
        let mountpoint = utils::flist_mount_path(&hash, &self.ro)?;
        match utils::valid(&mountpoint, &self.executor, &self.syscalls).await {
            Err(error) => match error.kind() {
                ErrorKind::AlreadyExists => return Ok(mountpoint),

                _ => bail!("validating of mount point failed"),
            },
            _ => {}
        };

        fs::create_dir_all(&mountpoint)?;
        let storage_url = match storage_url {
            Some(storage_url) => storage_url.as_ref().to_string(),
            None => {
                let environ = env::get()?;
                environ.storage_url
            }
        };

        let flist_path = utils::download_flist(url, &self.flist).await?;
        let log_name = hash + ".log";
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
            .arg(&mountpoint.as_os_str());
        self.executor.run(&cmd).await?;
        nix::unistd::sync();
        Ok(mountpoint.into())
    }
    async fn mount_overlay<T: AsRef<str>, P: AsRef<Path>>(
        &self,
        name: T,
        ro: P,
        opts: &MountOptions,
    ) -> Result<()> {
        let mountpoint = utils::mountpath(&name, &self.mountpoint)?;
        tokio::fs::create_dir_all(&mountpoint).await?;
        let permissions = Permissions::from_mode(0o755);
        fs::set_permissions(mountpoint.as_path(), permissions)?;
        if let MountMode::ReadWrite(WriteLayer::Size(limit)) = opts.mode {
            // no persisted volume provided, hence
            // we need to create one, or find one that is already exists
            let persistent = match self.storage.lookup(&name) {
                Ok(volume) => volume.path,
                Err(_) => {
                    // Volume doesn't exist create a new one
                    if limit == 0 {
                        bail!("Invalid mount option, missing disk type");
                    }
                    let path = match self.storage.create(&name, limit) {
                        Ok(volume) => volume.path,
                        Err(e) => {
                            self.storage.delete(&name)?;
                            bail!(e)
                        }
                    };
                    path
                }
            };
            let rw = persistent.join("rw");
            let wd = persistent.join("wd");
            let paths = vec![&rw, &wd];
            for path in paths {
                tokio::fs::create_dir_all(&path).await?;
                let permissions = Permissions::from_mode(0o755);
                fs::set_permissions(path.as_path(), permissions)?;
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
                mount::MsFlags::MS_NOATIME,
                Some(&data),
            )?;
        };
        Ok(())
    }
}

#[async_trait::async_trait]
impl<A, S, E> Flist for FListDaemon<A, S, E>
where
    A: VolumeAllocator + Sync + Send,
    S: Syscalls + Sync + Send,
    E: Executor + Sync + Send,
{
    async fn mount(&self, name: String, url: String, opts: MountOptions) -> Result<PathBuf> {
        let mountpoint = utils::mountpath(&name, &self.mountpoint)?;

        match utils::valid(&mountpoint, &self.executor, &self.syscalls).await {
            Err(error) => {
                if error.to_string().contains("path is already mounted") {
                    return Ok(mountpoint);
                } else {
                    bail!("validating of mount point failed");
                }
            }
            _ => {}
        };
        let ro = self.mount_ro(&url, opts.storage.clone()).await?;
        match &opts.mode {
            MountMode::ReadOnly => {
                mount_bind(&name, ro, &mountpoint, &self.syscalls, &self.executor).await?;
                return Ok(mountpoint);
            }
            MountMode::ReadWrite(_) => {
                self.mount_overlay(name, ro, &opts).await?;
                return Ok(mountpoint);
            }
        }
        //cleanup unused mounts
    }

    async fn unmount(name: String) -> Result<()> {
        unimplemented!()
    }

    async fn update(name: String, size: crate::Unit) -> Result<String> {
        unimplemented!()
    }

    async fn hash_of_mount(name: String) -> Result<String> {
        unimplemented!()
    }

    async fn exists(name: String) -> Result<bool> {
        unimplemented!()
    }
}
