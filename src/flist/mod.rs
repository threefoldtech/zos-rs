mod mgr;
/// implementation of the flist daemon
mod utils;
mod volume_allocator;
use crate::bus::api::Flist;
use crate::bus::types::storage::MountOptions;
use crate::env;
use crate::system::Command;
use crate::system::Executor;

use anyhow::{bail, Result};
use nix;
use std::fs;
use std::fs::Permissions;
use std::io;

use std::io::ErrorKind;
use std::os::unix::prelude::PermissionsExt;
use std::path::{Path, PathBuf};

use self::mgr::DiskMgr;
use self::volume_allocator::VolumeAllocator;

pub struct FListDaemon<A, D, E>
where
    A: VolumeAllocator,
    D: DiskMgr,
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
    disk_mgr: D,
    storage: A,
    executor: E,
}

impl<A, D, E> FListDaemon<A, D, E>
where
    A: VolumeAllocator,
    D: DiskMgr,
    E: Executor,
{
    fn new<R: Into<PathBuf>>(root: R, disk_mgr: D, storage: A, executor: E) -> Self
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
            disk_mgr,
            storage,
            executor,
        }
    }

    // MountRO mounts an flist in read-only mode. This mount then can be shared between multiple rw mounts
    // TODO: how to know that this ro mount is no longer used, hence can be unmounted and cleaned up?
    async fn mount_ro(&self, url: &str, storage: Option<String>) -> Result<PathBuf> {
        // this should return always the flist mountpoint. which is used
        // as a base for all RW mounts.
        let hash = match utils::hash_of_flist(url).await {
            Ok(hash) => hash,
            Err(_) => bail!("Failed to get flist hash"),
        };
        let mountpoint = utils::flist_mount_path(&hash, &self.ro)?;
        match utils::valid(&mountpoint, &self.executor, &self.disk_mgr).await {
            Err(error) => match error.kind() {
                ErrorKind::AlreadyExists => return Ok(mountpoint),

                _ => bail!("validating of mount point failed"),
            },
            _ => {}
        };

        fs::create_dir_all(&mountpoint)?;
        let storage = match storage {
            Some(storage) => storage,
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
            .arg(storage)
            .arg("--daemon")
            .arg("--log")
            .arg(log_path.as_os_str())
            .arg("--ro")
            .arg(&mountpoint.as_os_str());
        self.executor.run(&cmd).await?;
        nix::unistd::sync();
        Ok(mountpoint.into())
    }
}

#[async_trait::async_trait]
impl<A, D, E> Flist for FListDaemon<A, D, E>
where
    A: VolumeAllocator + Sync + Send,
    D: DiskMgr + Sync + Send,
    E: Executor + Sync + Send,
{
    async fn mount(&self, name: String, url: String, options: MountOptions) -> Result<PathBuf> {
        let mountpoint = utils::mountpath(name, &self.mountpoint)?;

        match utils::valid(&mountpoint, &self.executor, &self.disk_mgr).await {
            Err(error) => {
                if error.to_string().contains("path is already mounted") {
                    return Ok(mountpoint);
                } else {
                    bail!("validating of mount point failed");
                }
            }
            _ => {}
        };
        self.mount_ro(&url, options.storage).await?;
        //cleanup unused mounts
        Ok(mountpoint)
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

// #[cfg(test)]
// mod test {
//     use crate::bus::api::Flist;

//     use super::FListDaemon;

//     #[tokio::test]
//     async fn test_hash_of_flist() {
//         let flist_url = String::from("https://hub.grid.tf/tf-bootable/ubuntu:16.04.flist");

//         let hash = FListDaemon::hash_of_flist(flist_url).await.unwrap();
//         assert_eq!(hash, "17f8a26d538e5c502564381943a2feb0");
//     }
// }
