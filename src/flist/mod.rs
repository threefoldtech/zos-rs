/// implementation of the flist daemon
mod db;
mod mount;
mod volume_allocator;
use crate::bus::api::Flist;
use crate::bus::types::storage::MountMode;
use crate::bus::types::storage::MountOptions;

use crate::bus::types::storage::WriteLayer;
use crate::system::Executor;
use crate::system::Syscalls;

use crate::env;
use anyhow::bail;
use anyhow::Result;
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

        let ro_mount_path = self
            .mount_mgr
            .mount_ro(&url, opts.storage.unwrap_or(env::get()?.storage_url))
            .await?;

        match &opts.mode {
            MountMode::ReadOnly => {
                self.mount_mgr
                    .mount_bind(ro_mount_path, &mountpoint)
                    .await?;
            }

            MountMode::ReadWrite(write_layer) => {
                let rw = match write_layer {
                    WriteLayer::Size(size) => self.mount_mgr.get_volume_path(name, *size).await?,
                    WriteLayer::Path(path) => path.to_path_buf(),
                };

                self.mount_mgr
                    .mount_overlay(ro_mount_path, rw, &mountpoint)
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
        self.mount_mgr.storage.volume_delete(&name)?;
        self.mount_mgr.clean_unused_mounts().await
    }

    async fn update(&self, name: String, size: crate::Unit) -> Result<PathBuf> {
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        if !self.mount_mgr.is_mounted(&mountpoint).await {
            bail!("failed to update mountpoint is invalid")
        }
        self.mount_mgr.storage.volume_update(&name, size)?;
        Ok(mountpoint)
    }

    // returns the hash of the given flist name
    async fn hash_of_mount(&self, name: String) -> Result<String> {
        let mountpoint = self.mount_mgr.mountpath(&name)?;
        let pid = self.mount_mgr.resolve(&mountpoint).await?;
        let path = PathBuf::from("/proc").join(pid.to_string()).join("cmdline");

        let cmdline = fs::read_to_string(path).await?;

        let parts = cmdline.split('\0');
        for part in parts {
            let path = PathBuf::from(&part);
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

#[cfg(test)]
mod test {

    use super::{FListDaemon, MountManager};
    use crate::bus::api::Flist;
    use crate::bus::types::storage::{MountMode, MountOptions};
    use crate::{
        flist::volume_allocator::MockVolumeAllocator,
        system::{Command, Mockyscalls},
    };
    #[tokio::test]
    async fn test_mount_with_mount_bind() {
        let executor = crate::system::MockExecutor::default();

        let mut flist = FListDaemon::new("/tmp/flist", Mockyscalls, MockVolumeAllocator, executor)
            .await
            .unwrap();
        let flist_path = flist
            .mount_mgr
            .flist
            .join("efc9269253cb7210d6eded4aa53b7dfc");
        let ro_mountpoint = flist.mount_mgr.ro.join("efc9269253cb7210d6eded4aa53b7dfc");
        let log_path = flist
            .mount_mgr
            .log
            .join("efc9269253cb7210d6eded4aa53b7dfc.log");
        let storage_url = "http://storage-url.com";
        let cmd = Command::new("g8ufs")
            .arg("--cache")
            .arg(flist.mount_mgr.cache.as_os_str())
            .arg("--meta")
            .arg(flist_path)
            .arg("--storage-url")
            .arg(storage_url)
            .arg("--daemon")
            .arg("--log")
            .arg(log_path.as_os_str())
            .arg("--ro")
            .arg(&ro_mountpoint.as_os_str());
        flist
            .mount_mgr
            .executor
            .expect_run()
            .times(1)
            .withf(move |arg: &Command| arg == &cmd)
            .returning(|_| Ok(Vec::default()));
        let opts = MountOptions {
            mode: MountMode::ReadOnly,
            storage: Some(storage_url.to_string()),
        };
        match flist
            .mount(
                "test_flist".into(),
                "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist".into(),
                opts,
            )
            .await
        {
            // /tmp/flist/mountpoint/test_flist, was not mounted in time
            Ok(_) => {}
            Err(error) => {
                assert_eq!(
                    error.to_string(),
                    "/tmp/flist/mountpoint/test_flist, was not mounted in time"
                )
            }
        }
    }
}
