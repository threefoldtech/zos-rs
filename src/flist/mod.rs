/// implementation of the flist daemon
use crate::bus::api::Flist;
use crate::bus::types::storage::MountOptions;
use crate::env;
use crate::Unit;
use anyhow::{bail, Result};
use execute::Execute;
use linux::kty::MS_BIND;
use linux::syscall;
use md5::{Digest, Md5};
use std::fs;
use std::fs::File;
use std::fs::Permissions;
use std::io;
use std::io::{Read, Write};
use std::os::unix::prelude::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Output;
use tempfile;

pub struct FListDaemon<A, S, C>
where
    A: VolumeAllocator,
    S: System,
    C: Commander,
{
    // root directory where all
    // the working file of the module will be located
    root: String,

    // underneath are the path for each
    // sub folder used by the flist module
    flist: String,
    cache: String,
    mountpoint: String,
    ro: String,
    pid: String,
    log: String,
    system: S,
    storage: A,
    commander: C,
}

impl<A, S, C> FListDaemon<A, S, C>
where
    A: VolumeAllocator,
    S: System,
    C: Commander,
{
    fn new<R>(root: R, system: S, storage: A, commander: C) -> Self
    where
        R: AsRef<str>,
    {
        let root = Path::new(root.as_ref());
        Self {
            root: root.to_string_lossy().into(),
            flist: root.join("flist").to_string_lossy().into(),
            cache: root.join("cache").to_string_lossy().into(),
            mountpoint: root.join("mountpoint").to_string_lossy().into(),
            ro: root.join("ro").to_string_lossy().into(),
            pid: root.join("pid").to_string_lossy().into(),
            log: root.join("log").to_string_lossy().into(),
            system,
            storage,
            commander,
        }
    }
    pub fn mountpath(&self, name: String) -> Result<PathBuf> {
        let mountpath = Path::new(&self.mountpoint).join(&name);
        if mountpath.parent() != Some(Path::new(&self.mountpoint)) {
            bail!("inavlid mount name: {}", &name);
        }
        Ok(mountpath)
    }

    fn is_mountpoint<P: AsRef<Path>>(&self, path: P) -> Result<Output> {
        if let Some(path_str) = path.as_ref().as_os_str().to_str() {
            self.commander
                .execute("mountpoint", vec![path_str.to_string()])
        } else {
            bail!("Invalid path")
        }
    }
    pub fn valid(&self, path: &str) -> Result<()> {
        match fs::metadata(path) {
            Ok(info) => {
                if !info.is_dir() {
                    bail!("{} is not a directory", path);
                }
                if let Ok(_) = self.is_mountpoint(path) {
                    bail!("path is already mounted")
                }
            }
            Err(error) => {
                let error_str = error.to_string();
                if error_str.contains("No such file or directory (os error 2)") {
                    return Ok(());
                } else if error_str.contains("transport endpoint is not connected") {
                    return self.system.unmount(String::from(path), 0);
                } else {
                    bail!("failed to check mountpoint: {}", path)
                }
            }
        };
        Ok(())
    }
    fn flist_mount_path(&self, hash: String) -> Result<PathBuf> {
        let mountpath = Path::new(&self.ro).join(&hash);
        if mountpath.parent() != Some(Path::new(&self.ro)) {
            bail!("invalid mount name")
        }

        Ok(mountpath)
    }
    fn compare_md5<R: Read, P: AsRef<str>>(&self, hash: P, mut reader: R) -> Result<bool> {
        // create a Md5 hasher instance
        let mut hasher = Md5::new();
        io::copy(&mut reader, &mut hasher)?;
        let calculated_hash = hasher.finalize();
        let calculated_hash = base16ct::lower::encode_string(&calculated_hash);

        Ok(calculated_hash == hash.as_ref().to_string())
    }
    fn save_flist(&self, content: String) -> Result<String> {
        let mut builder = tempfile::Builder::new();
        let mut file = builder.suffix("_flist_temp").tempfile_in(&self.flist)?;

        let mut hasher = Md5::new();
        io::copy(&mut content.as_bytes(), &mut file)?;
        io::copy(&mut file, &mut hasher)?;
        let calculated_hash = hasher.finalize();
        let hash = base16ct::lower::encode_string(&calculated_hash);
        let path = Path::new(&self.flist).join(&hash);
        let tmp_path = &file.path();
        let permissions = Permissions::from_mode(0755);
        fs::set_permissions(&path, permissions)?;
        if let Some(parent_dir) = path.parent() {
            fs::create_dir_all(parent_dir)?;
            fs::rename(&tmp_path, &path)?;
        } else {
            bail!(
                "Can not create parent directory of {}",
                path.display().to_string()
            )
        }
        Ok(path.display().to_string())
    }
    fn mount_bind(&self, name: String, ro: String) -> Result<bool> {
        let mountpoint = self.mountpath(name)?;
        fs::create_dir_all(mountpoint)?;
        let permissions = Permissions::from_mode(0755);
        fs::set_permissions(mountpoint, permissions)?;
        let success = self.system.mount(
            ro,
            mountpoint.display().to_string(),
            "bind".into(),
            MS_BIND,
            "".into(),
        );
        Ok(true)
    }
}

#[async_trait::async_trait]
impl<A, S, C> Flist for FListDaemon<A, S, C>
where
    A: VolumeAllocator + Sync,
    S: System + Sync,
    C: Commander + Sync,
{
    // MountRO mounts an flist in read-only mode. This mount then can be shared between multiple rw mounts
    // TODO: how to know that this ro mount is no longer used, hence can be unmounted and cleaned up?
    async fn mount_ro(&self, url: &str, storage: &str) -> Result<String> {
        // this should return always the flist mountpoint. which is used
        // as a base for all RW mounts.
        let hash = match self.hash_of_flist(url).await {
            Ok(hash) => hash,
            Err(_) => bail!("Failed to get flist hash"),
        };
        let mountpoint = self.flist_mount_path(hash)?;
        let mountpoint = match mountpoint.to_str() {
            Some(mountpoint) => mountpoint,
            None => bail!("mountpoint is None"),
        };
        match self.valid(&mountpoint) {
            Err(error) => {
                if error.to_string().contains("path is already mounted") {
                    return Ok(mountpoint.to_string());
                } else {
                    bail!("validating of mount point failed");
                }
            }
            _ => {}
        };

        fs::create_dir_all(mountpoint)?;
        if storage == "" {
            let environ = env::get()?;
            let storage = environ.storage_url;
        }
        let flist_path = self.download_flist(url).await?;
        let log_name = hash + ".log";
        let log_path = Path::new(&self.log).join(&log_name);

        let args = vec![
            "--cache".into(),
            self.cache,
            "--meta".into(),
            flist_path,
            "--storage-url".into(),
            storage.into(),
            "--daemon".into(),
            "--log".into(),
            log_path.display().to_string(),
            // this is always read-only
            "--ro".into(),
            mountpoint.into(),
        ];
        self.commander.execute("g8ufs", args)?;
        syscall::sync();
        Ok(mountpoint.into())
    }

    async fn mount(&self, name: String, url: String, options: MountOptions) -> Result<String> {
        let mountpoint = self.mountpath(name)?;
        let mountpoint = match mountpoint.to_str() {
            Some(mountpoint) => mountpoint,
            None => bail!("mountpoint is None"),
        };
        match self.valid(&mountpoint) {
            Err(error) => {
                if error.to_string().contains("path is already mounted") {
                    return Ok(mountpoint.to_string());
                } else {
                    bail!("validating of mount point failed");
                }
            }
            _ => {}
        };
        //mountRO
        //cleanup unused mounts
        Ok(String::from("success"))
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
    // downloadFlist downloads an flits from a URL
    // if the flist location also provide and md5 hash of the flist
    // this function will use it to avoid downloading an flist that is
    // already present locally
    async fn download_flist(&self, url: &str) -> Result<String> {
        // first check if the md5 of the flist is available
        let hash = self.hash_of_flist(&url).await?;
        let path = Path::new(&self.flist).join(&hash.trim());
        match File::open(&path) {
            Ok(file) => {
                //Flist already exists let's check it's md5
                if self.compare_md5(hash, file)? {
                    return Ok(path.display().to_string());
                }
            }
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => {}

                _ => bail!(
                    "Error reading flist file: {}, error {}",
                    &path.display().to_string(),
                    error
                ),
            },
        };
        // Flist not found or hash is not correct, let's download
        let body = reqwest::get(url).await?.text().await?;
        return self.save_flist(body);
    }

    async fn hash_of_flist(&self, url: &str) -> Result<String> {
        let md5_url = format!("{url}.md5");
        let res = reqwest::get(md5_url)
            .await?
            .text()
            .await?
            .trim()
            .to_string();

        Ok(res)
    }

    async fn exists(name: String) -> Result<bool> {
        unimplemented!()
    }
}

pub trait System {
    fn mount(
        &self,
        source: String,
        target: String,
        fstype: String,
        flags: u64,
        data: String,
    ) -> Result<String>;

    /// unmount mount with name
    fn unmount(&self, target: String, flags: u64) -> Result<()>;
}
struct DefaultSystem;
impl System for DefaultSystem {
    fn mount(
        &self,
        source: String,
        target: String,
        fstype: String,
        flags: u64,
        data: String,
    ) -> Result<i32> {
        let status = syscall::mount(source, target, fstype, flags, data);
        if status == 0 {
            return Ok(status);
        } else {
            return Err(status);
        }
    }

    fn unmount(&self, target: String, flags: u64) -> Result<i32> {
        let status = syscall::umount(target, flags);
        if status == 0 {
            return Ok(status);
        } else {
            return Err(status);
        }
    }
}

pub struct Usage {
    size: Unit,
    used: Unit,
}

// Volume struct is a btrfs subvolume
pub struct Volume {
    name: String,
    path: String,
    usage: Usage,
}

pub trait VolumeAllocator {
    // CreateFilesystem creates a filesystem with a given size. The filesystem
    // is mounted, and the path to the mountpoint is returned. The filesystem
    // is only attempted to be created in a pool of the given type. If no
    // more space is available in such a pool, `ErrNotEnoughSpace` is returned.
    // It is up to the caller to handle such a situation and decide if he wants
    // to try again on a different devicetype
    fn create(name: String, size: Unit) -> Result<Volume>;

    // VolumeUpdate changes the size of an already existing volume
    fn update(name: String, size: Unit) -> Result<()>;

    // ReleaseFilesystem signals that the named filesystem is no longer needed.
    // The filesystem will be unmounted and subsequently removed.
    // All data contained in the filesystem will be lost, and the
    // space which has been reserved for this filesystem will be reclaimed.
    fn delete(cname: String) -> Result<()>;
    // Path return the filesystem named name
    // if no filesystem with this name exists, an error is returned
    fn lookup(name: String) -> Result<Volume>;
}

pub trait Commander {
    fn execute(&self, name: &str, args: Vec<String>) -> Result<Output>;
}

struct DefaultCommander;
impl Commander for DefaultCommander {
    fn execute(&self, name: String, args: Vec<String>) -> Result<Output> {
        let mut command = Command::new(&name);
        for arg in args {
            command.arg(&arg);
        }
        Ok(command.execute_output()?)
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
