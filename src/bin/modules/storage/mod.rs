use anyhow::{Context, Result};
use zos::storage::{self, Manager};
use zos::system::{MsFlags, Syscalls, System};

const CACHE_VOL: &str = "zos-cache";
const CACHE_MNT: &str = "/var/cache";
const CACHE_SIZE: zos::Unit = 100 * zos::GIGABYTE;

/// entry point for storage d
pub async fn run<P: AsRef<str>>(_broker: P) -> Result<()> {
    let lsblk = storage::device::LsBlk::default();
    let btrfs = storage::pool::BtrfsManager::default();
    let mut mgr = storage::StorageManager::new(lsblk, btrfs)
        .await
        .context("failed to initialize storage manager")?;

    setup_cache(&mut mgr).await?;
    Ok(())
}

async fn setup_cache<M: Manager>(mgr: &mut M) -> Result<()> {
    tokio::fs::create_dir_all(CACHE_MNT)
        .await
        .context("failed to create cache directory")?;

    let info = storage::mountpoint(CACHE_MNT)
        .await
        .context("failed to check cache mount")?;

    // cache is already mounted, nothing to do
    if info.is_some() {
        return Ok(());
    }

    let vol = mgr
        .volume_create(CACHE_VOL, CACHE_SIZE)
        .await
        .context("failed to allocate cache volume")?;

    System
        .mount(
            Some(vol.path),
            CACHE_MNT,
            Option::<&str>::None,
            MsFlags::MS_BIND,
            Option::<&str>::None,
        )
        .context("failed to mount cache volume")?;

    Ok(())
}
