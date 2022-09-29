use super::mounts;
use super::volume_allocator::VolumeAllocator;
use crate::bus::types::storage::{MountMode, MountOptions, WriteLayer};
use crate::env;
use crate::system::{Command, Executor, Syscalls};
use anyhow::{bail, Result};
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use std::io::{self, Error, ErrorKind};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::time;
pub struct MetadataDbMgr<A, S, E>
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
    log: PathBuf,
    syscalls: S,
    storage: A,
    executor: E,
}
impl<A, S, E> MetadataDbMgr<A, S, E>
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
    {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        // prepare directory layout for the module
        for path in &["flist", "cache", "mountpoint", "ro", "pid", "log"] {
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
    pub fn flist_ro_mount_path<R: AsRef<str>>(&self, hash: R) -> Result<PathBuf> {
        let mountpath = self.ro.join(hash.as_ref());
        if mountpath.parent() != Some(self.ro.as_path()) {
            bail!("invalid mount name")
        }

        Ok(mountpath)
    }
    // Checks if the given path is mountpoint or not
    pub async fn is_mounted<P: AsRef<Path>>(&self, path: P) -> bool {
        if let Some(path_str) = path.as_ref().as_os_str().to_str() {
            let cmd = Command::new("mountpoint").arg(path_str);
            return self.executor.run(&cmd).await.is_ok();
        }
        false
    }

    // checks if flist exists
    pub async fn exists(&self, name: String) -> Result<bool> {
        let mountpoint = self.mountpath(name)?;
        self.valid(&mountpoint).await?;
        return Ok(true);
    }
    // Checks is the given path is a valid mountpoint means:
    // it is either doesn't exist or
    // it is a dir and not a mountpoint for anything
    pub async fn valid<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        match fs::metadata(&path).await {
            Ok(info) => {
                if !info.is_dir() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("{} is not a directory", path.as_ref().display().to_string()),
                    ));
                }

                if self.is_mounted(&path).await {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        format!("{} is already mounted", path.as_ref().display().to_string()),
                    ));
                } else {
                    return Ok(());
                }
            }
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => return Ok(()),
                // transport endpoint is not connected
                io::ErrorKind::ConnectionAborted => match self.syscalls.umount(path, None) {
                    Ok(_) => return Ok(()),
                    Err(_) => return Err(Error::new(ErrorKind::Other, "can not do unmount")),
                },
                _ => return Err(Error::new(ErrorKind::Other, "Failed to check mount point")),
            },
        };
    }
    // get's flish hash from hub
    pub async fn hash_of_flist<T: AsRef<str>>(&self, url: T) -> Result<String> {
        let md5_url = format!("{}.md5", url.as_ref());
        let res = reqwest::get(md5_url)
            .await?
            .text()
            .await?
            .trim()
            .to_string();
        Ok(res)
    }
    // returns hash of mounted flish from its path
    pub async fn hash_of_mount<T: AsRef<str>>(&self, name: T) -> Result<String> {
        {
            let mountpoint = self.mountpath(&name)?;
            let info = mounts::resolve(&mountpoint, &self.executor).await?;
            let path = Path::new("/proc")
                .join(info.pid.to_string())
                .join("cmdline");

            let cmdline = fs::read_to_string(path).await?;

            let parts = cmdline.split("\0");
            for part in parts {
                let path = Path::new(&part);
                if path.starts_with(&self.flist) {
                    match path.file_name() {
                        Some(filename) => return Ok(filename.to_string_lossy().to_string()),
                        None => bail!("Failed to get hash for this mount"),
                    }
                }
            }
            bail!("Failed to get hash for this mount")
        }
    }
    pub fn compare_md5<T: AsRef<str>, P: AsRef<Path>>(&self, hash: T, path: P) -> bool {
        // create a Md5 hasher instance
        let calculated_hash =
            checksums::hash_file(path.as_ref(), checksums::Algorithm::MD5).to_lowercase();
        calculated_hash == hash.as_ref()
    }
    // downloadFlist downloads an flits from a URL
    // if the flist location also provide and md5 hash of the flist
    // this function will use it to avoid downloading an flist that is
    // already present locally
    pub async fn download_flist<P: AsRef<Path>, T: AsRef<str>>(
        &self,
        url: T,
        flist_path: P,
    ) -> Result<PathBuf> {
        // first check if the md5 of the flist is available
        let url = url.as_ref();
        let hash = self.hash_of_flist(url).await?;
        let path = flist_path.as_ref().join(&hash.trim());
        match File::open(&path).await {
            Ok(_) => {
                //Flist already exists let's check it's md5
                if self.compare_md5(hash, &path) {
                    return Ok(path);
                }
            }
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => {}

                _ => bail!(
                    "Error reading flist file: {}, error {}",
                    &path.display(),
                    error
                ),
            },
        };
        // Flist not found or hash is not correct, let's download
        let mut resp = reqwest::get(url).await?.bytes().await?.reader();
        self.save_flist(&mut resp, flist_path.as_ref()).await
    }

    pub async fn save_flist<P: AsRef<Path>>(
        &self,
        resp: &mut Reader<Bytes>,
        flist_path: P,
    ) -> Result<PathBuf> {
        let mut builder = tempfile::Builder::new();
        let mut file = builder.suffix("_flist_temp").tempfile_in(&flist_path)?;
        io::copy(resp, &mut file)?;
        let tmp_path = file.path();
        let hash =
            checksums::hash_file(Path::new(&tmp_path), checksums::Algorithm::MD5).to_lowercase();
        let path = flist_path.as_ref().join(&hash);
        if let Some(parent_dir) = path.parent() {
            fs::create_dir_all(parent_dir).await?;
        } else {
            bail!(
                "Can not create parent directory of {}",
                path.display().to_string()
            )
        }
        fs::rename(tmp_path, &path).await?;
        Ok(path)
    }
    pub async fn wait_mountpoint<P: AsRef<Path>>(&self, path: P, seconds: u32) -> Result<()> {
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
        let hash = match self.hash_of_flist(&url).await {
            Ok(hash) => hash,
            Err(_) => bail!("Failed to get flist hash"),
        };
        let ro_mountpoint = self.flist_ro_mount_path(&hash)?;
        if self.is_mounted(&ro_mountpoint).await {
            return Ok(ro_mountpoint);
        }
        self.valid(&ro_mountpoint).await?;

        fs::create_dir_all(&ro_mountpoint).await?;
        let storage_url = match storage_url {
            Some(storage_url) => storage_url.as_ref().to_string(),
            None => {
                let environ = env::get()?;
                environ.storage_url
            }
        };

        let flist_path = self.download_flist(url, &self.flist).await?;
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
        if let Err(_) = self.syscalls.mount(
            Some(ro_mount_path),
            &mountpoint,
            Some("bind"),
            nix::mount::MsFlags::MS_BIND,
            Option::<&str>::None,
        ) {
            if let Err(err) = self.syscalls.umount(&mountpoint, None) {
                log::debug!(
                    "Failed to unmount {}, Error: {}",
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
    pub async fn unmount<P: AsRef<str>>(&self, name: P) -> Result<()> {
        let mountpoint = self.mountpath(&name)?;
        if let Err(err) = self.valid(&mountpoint).await {
            match err.kind() {
                ErrorKind::AlreadyExists => self.syscalls.umount(&mountpoint, None)?,
                _ => {}
            }
        }
        fs::remove_dir_all(&mountpoint).await?;
        self.storage.delete(&name)?;
        return Ok(());
    }
    pub async fn clean_unused_mounts(&self) -> Result<()> {
        mounts::clean_unused_mounts(
            &self.root,
            &self.ro,
            &self.mountpoint,
            &self.executor,
            &self.syscalls,
        )
        .await
    }
    pub async fn update<T: AsRef<str>>(&self, name: T, size: crate::Unit) -> Result<PathBuf> {
        let mountpoint = self.mountpath(&name)?;
        if !self.is_mounted(&mountpoint).await {
            bail!("failed to update mountpoint is invalid")
        }
        self.storage.update(&name, size)?;
        Ok(mountpoint)
    }
}

mod test {
    use crate::{
        flist::volume_allocator::DummyVolumeAllocator,
        system::{self, Command, DummySyscalls, Syscalls, System},
    };

    use super::MetadataDbMgr;
    use std::{ffi::OsStr, path::Path};
    use tokio::fs;
    #[tokio::test]
    async fn test_download_flist() {
        let executor = crate::system::MockExecutor::default();
        let syscalls = DummySyscalls;
        let allocator = DummyVolumeAllocator;
        let metadata_mgr = MetadataDbMgr::new("/tmp/flist_test", syscalls, allocator, executor)
            .await
            .unwrap();

        fs::create_dir_all("/tmp/flist_test").await.unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let path = metadata_mgr
            .download_flist(url, &Path::new("/tmp/flist_test").to_path_buf())
            .await
            .unwrap();
        let filename = metadata_mgr.hash_of_flist(url).await.unwrap();
        // make sure the downloaded file matches the hash of this flist
        assert_eq!(Some(OsStr::new(&filename)), path.file_name());
        let first_file_created = fs::metadata(&path).await.unwrap().created().unwrap();
        let path = metadata_mgr
            .download_flist(url, &Path::new("/tmp/flist_test").to_path_buf())
            .await
            .unwrap();
        let second_file_created = fs::metadata(&path).await.unwrap().created().unwrap();
        // make sure the second file is not created because it is the same file
        assert_eq!(first_file_created, second_file_created);
        fs::remove_dir_all("/tmp/flist_test").await.unwrap();
    }
    #[tokio::test]
    async fn test_hash_of_flist() {
        let executor = crate::system::MockExecutor::default();
        let syscalls = DummySyscalls;
        let allocator = DummyVolumeAllocator;
        let metadata_mgr = MetadataDbMgr::new("/tmp/flist_test", syscalls, allocator, executor)
            .await
            .unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let hash = metadata_mgr.hash_of_flist(url).await.unwrap();
        assert_eq!(hash, "efc9269253cb7210d6eded4aa53b7dfc")
    }
}
