mod db;
mod mounts;
/// implementation of the flist daemon
mod volume_allocator;
use crate::bus::api::Flist;
use crate::bus::types::storage::MountMode;
use crate::bus::types::storage::MountOptions;

use crate::system::Executor;
use crate::system::Syscalls;

use anyhow::Result;
use std::path::PathBuf;

use self::db::MetadataDbMgr;
use self::volume_allocator::VolumeAllocator;

pub struct FListDaemon<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
    E: Executor + Sync + Send,
{
    db: MetadataDbMgr<A, S, E>,
}

impl<A, S, E> FListDaemon<A, S, E>
where
    A: VolumeAllocator,
    S: Syscalls,
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
        let db = db::MetadataDbMgr::new(root, syscalls, storage, executor).await?;
        Ok(Self { db })
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
        let mountpoint = self.db.mountpath(&name)?;
        if self.db.is_mounted(&mountpoint).await {
            return Ok(mountpoint);
        }
        self.db.valid(&mountpoint).await?;
        // let ro = self.mount_ro(&url, opts.storage.clone()).await?;
        let ro_mount_path = self.db.mount_ro(&url, opts.storage.clone()).await?;
        match &opts.mode {
            MountMode::ReadOnly => {
                self.db.mount_bind(ro_mount_path, &mountpoint).await?;
            }
            MountMode::ReadWrite(_) => {
                self.db
                    .mount_overlay(name, ro_mount_path, &mountpoint, &opts)
                    .await?;
            }
        }
        self.db.clean_unused_mounts().await?;
        Ok(mountpoint)
    }

    async fn unmount(&self, name: String) -> Result<()> {
        self.db.unmount(&name).await?;
        self.db.clean_unused_mounts().await
    }

    async fn update(&self, name: String, size: crate::Unit) -> Result<PathBuf> {
        let mountpoint = self.db.update(&name, size).await?;
        Ok(mountpoint)
    }
    // returns the hash of the given flist name
    async fn hash_of_mount(&self, name: String) -> Result<String> {
        self.db.hash_of_mount(name).await
    }

    async fn exists(&self, name: String) -> Result<bool> {
        self.db.exists(name).await
    }
}
