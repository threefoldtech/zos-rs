/// implementation of the flist daemon
mod db;
mod mount;
mod volume_allocator;
use crate::bus::api::Flist;
use crate::bus::types::storage::MountMode;
use crate::bus::types::storage::MountOptions;

use crate::system::Executor;
use crate::system::Syscalls;

use anyhow::bail;
use anyhow::Result;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;

use self::mount::MountManager;
use self::volume_allocator::VolumeAllocator;

pub struct FListDaemon<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
    E: Executor + Sync + Send,
{
    mount_mgr: MountManager<A, S, E>,
}
impl<A, S, E> FListDaemon<A, S, E>
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
        A: VolumeAllocator,
        S: Syscalls,
        E: Executor,
    {
        let mount_mgr = mount::MountManager::new(root, syscalls, storage, executor).await?;
        Ok(Self { mount_mgr })
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
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        if self.mount_mgr.is_mounted(&mountpoint).await {
            return Ok(mountpoint);
        }
        if !self.mount_mgr.valid(&mountpoint).await {
            bail!("invalid mountpoint {}", &mountpoint.display())
        }
        let ro_mount_path = self.mount_mgr.mount_ro(&url, opts.storage.clone()).await?;
        match &opts.mode {
            MountMode::ReadOnly => {
                self.mount_mgr
                    .mount_bind(ro_mount_path, &mountpoint)
                    .await?;
            }
            MountMode::ReadWrite(_) => {
                self.mount_mgr
                    .mount_overlay(name, ro_mount_path, &mountpoint, &opts)
                    .await?;
            }
        }
        self.mount_mgr.clean_unused_mounts().await?;
        Ok(mountpoint)
    }

    async fn unmount(&self, name: String) -> Result<()> {
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        if self.mount_mgr.is_mounted(&mountpoint).await {
            self.mount_mgr.syscalls.umount(&mountpoint, None)?
        }

        fs::remove_dir_all(&mountpoint).await?;
        self.mount_mgr.storage.delete(&name)?;
        self.mount_mgr.clean_unused_mounts().await
    }

    async fn update(&self, name: String, size: crate::Unit) -> Result<PathBuf> {
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        if !self.mount_mgr.is_mounted(&mountpoint).await {
            bail!("failed to update mountpoint is invalid")
        }
        self.mount_mgr.storage.update(&name, size)?;
        Ok(mountpoint)
    }

    // returns the hash of the given flist name
    async fn hash_of_mount(&self, name: String) -> Result<String> {
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        let info = self.mount_mgr.resolve(&mountpoint).await?;
        let path = Path::new("/proc")
            .join(info.pid.to_string())
            .join("cmdline");

        let cmdline = fs::read_to_string(path).await?;

        let parts = cmdline.split('\0');
        for part in parts {
            let path = Path::new(&part);
            if path.starts_with(&self.mount_mgr.flist) {
                match path.file_name() {
                    Some(filename) => return Ok(filename.to_string_lossy().to_string()),
                    None => bail!("Failed to get hash for this mount"),
                }
            }
        }
        bail!("failed to get hash for this mount")
    }

    async fn exists(&self, name: String) -> Result<bool> {
        let mountpoint = self.mount_mgr.mountpath(name)?;
        Ok(self.mount_mgr.is_mounted(&mountpoint).await)
    }
}
