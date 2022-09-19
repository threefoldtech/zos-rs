use std::path::PathBuf;

use anyhow::Result;

pub trait DiskMgr {
    fn mount(
        &self,
        source: Option<&PathBuf>,
        target: &PathBuf,
        fstype: Option<&str>,
        flags: nix::mount::MsFlags,
        data: Option<&str>,
    ) -> Result<()>;

    /// unmount mount with name
    fn unmount(&self, target: &PathBuf) -> Result<()>;
}
pub struct DefaultDiskMgr;
impl DiskMgr for DefaultDiskMgr {
    fn mount(
        &self,
        source: Option<&PathBuf>,
        target: &PathBuf,
        fstype: Option<&str>,
        flags: nix::mount::MsFlags,
        data: Option<&str>,
    ) -> Result<()> {
        nix::mount::mount(source, target, fstype, flags, data)?;
        Ok(())
    }

    fn unmount(&self, target: &PathBuf) -> Result<()> {
        nix::mount::umount(target)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::{self, File},
        path::{Path, PathBuf},
    };

    use crate::flist::mgr::DefaultDiskMgr;
    use crate::flist::mgr::DiskMgr;

    #[tokio::test]
    async fn test_mount() {
        let dir_path = "/tmp/mount_test_src";
        let mount_path = "/tmp/mount_test_dst";
        let file_name = "test_file.txt";
        fs::create_dir_all(dir_path).unwrap();
        fs::create_dir_all(mount_path).unwrap();
        let file_path = Path::new(dir_path).join(file_name);
        File::create(&file_path).unwrap();
        let mgr = DefaultDiskMgr;
        mgr.mount(
            Some(&PathBuf::from(dir_path)),
            &PathBuf::from(mount_path),
            Some("bind"),
            nix::mount::MsFlags::MS_BIND,
            None,
        )
        .unwrap();
        let mount_file_path = Path::new(mount_path).join(file_name);
        assert!(mount_file_path.exists());
        mgr.unmount(&PathBuf::from(mount_path)).unwrap();
        assert!(!mount_file_path.exists());
        fs::remove_dir_all("/tmp/mount_test_src").unwrap();
        fs::remove_dir_all("/tmp/mount_test_dst").unwrap();
    }
}
