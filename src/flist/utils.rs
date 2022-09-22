use crate::system::{Command, Executor, Syscalls};
use anyhow::{bail, Result};
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use std::io::{Error, ErrorKind};
use std::os::unix::prelude::PermissionsExt;
use std::path::PathBuf;
use std::{
    fs::{self, File, Permissions},
    io,
    path::Path,
};
use tokio::time;

// downloadFlist downloads an flits from a URL
// if the flist location also provide and md5 hash of the flist
// this function will use it to avoid downloading an flist that is
// already present locally
pub async fn download_flist<P: AsRef<Path>, T: AsRef<str>>(
    url: T,
    flist_path: P,
) -> Result<PathBuf> {
    // first check if the md5 of the flist is available
    let url = url.as_ref();
    let hash = hash_of_flist(url).await?;
    let path = flist_path.as_ref().join(&hash.trim());
    match File::open(&path) {
        Ok(_) => {
            //Flist already exists let's check it's md5
            if compare_md5(hash, &path)? {
                return Ok(path);
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
    let mut resp = reqwest::get(url).await?.bytes().await?.reader();
    return save_flist(&mut resp, flist_path.as_ref()).await;
}

pub async fn save_flist<P: AsRef<Path>>(
    resp: &mut Reader<Bytes>,
    flist_path: P,
) -> Result<PathBuf> {
    let mut builder = tempfile::Builder::new();
    let mut file = builder.suffix("_flist_temp").tempfile_in(&flist_path)?;
    io::copy(resp, &mut file)?;
    let tmp_path = file.path();
    let hash = checksums::hash_file(Path::new(&tmp_path), checksums::Algorithm::MD5).to_lowercase();
    let path = flist_path.as_ref().join(&hash);
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir)?;
        let permissions = Permissions::from_mode(0o755);
        fs::set_permissions(&parent_dir, permissions)?;
    } else {
        bail!(
            "Can not create parent directory of {}",
            path.display().to_string()
        )
    }
    fs::rename(tmp_path, &path)?;
    Ok(path)
}

pub async fn hash_of_flist<S: AsRef<str>>(url: S) -> Result<String> {
    let md5_url = format!("{}.md5", url.as_ref());
    let res = reqwest::get(md5_url)
        .await?
        .text()
        .await?
        .trim()
        .to_string();
    Ok(res)
}

pub fn compare_md5<S: AsRef<str>, P: AsRef<Path>>(hash: S, path: P) -> Result<bool> {
    // create a Md5 hasher instance
    let calculated_hash =
        checksums::hash_file(path.as_ref(), checksums::Algorithm::MD5).to_lowercase();
    Ok(calculated_hash == hash.as_ref())
}

pub fn mountpath<S: AsRef<str>, P: AsRef<Path>>(name: S, mountpoint_path: P) -> Result<PathBuf> {
    let mountpath = Path::new(mountpoint_path.as_ref()).join(name.as_ref());
    if mountpath.parent() != Some(mountpoint_path.as_ref()) {
        bail!("inavlid mount name: {}", name.as_ref());
    }
    Ok(mountpath)
}

pub fn flist_mount_path<P: AsRef<Path>, S: AsRef<str>>(hash: S, ro_path: P) -> Result<PathBuf> {
    let mountpath = ro_path.as_ref().join(hash.as_ref());
    if mountpath.parent() != Some(ro_path.as_ref()) {
        bail!("invalid mount name")
    }

    Ok(mountpath)
}
pub async fn is_mountpoint<E: Executor, P: AsRef<Path>>(path: P, executor: &E) -> Result<Vec<u8>> {
    if let Some(path_str) = path.as_ref().as_os_str().to_str() {
        let cmd = Command::new("mountpoint").arg(path_str);
        Ok(executor.run(&cmd).await?)
    } else {
        bail!("Invalid path")
    }
}

pub async fn valid<E: Executor, S: Syscalls, P: AsRef<Path>>(
    path: P,
    executor: &E,
    syscalls: &S,
) -> Result<(), Error> {
    match fs::metadata(&path) {
        Ok(info) => {
            if !info.is_dir() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("{} is not a directory", path.as_ref().display().to_string()),
                ));
            }
            match is_mountpoint(&path, executor).await {
                Ok(_) => {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        format!("{} is already mounted", path.as_ref().display().to_string()),
                    ));
                }
                _ => return Ok(()),
            }
        }
        Err(error) => match error.kind() {
            io::ErrorKind::NotFound => return Ok(()),
            // transport endpoint is not connected
            io::ErrorKind::ConnectionAborted => match syscalls.umount(path, None) {
                Ok(_) => return Ok(()),
                Err(_) => return Err(Error::new(ErrorKind::Other, "can not do unmount")),
            },
            _ => return Err(Error::new(ErrorKind::Other, "Failed to check mount point")),
        },
    };
}
pub async fn mount_bind<N: AsRef<str>, S: Syscalls, E: Executor, P: AsRef<Path>, T: AsRef<Path>>(
    name: N,
    ro: P,
    mountpoint: T,
    syscalls: &S,
    executor: &E,
) -> Result<bool> {
    let mountpoint = mountpath(name, mountpoint)?;
    fs::create_dir_all(&mountpoint)?;
    let permissions = Permissions::from_mode(0755);
    fs::set_permissions(&mountpoint, permissions)?;
    if let Err(_) = syscalls.mount(
        Some(ro),
        &mountpoint,
        Some("bind"),
        nix::mount::MsFlags::MS_BIND,
        Option::<&str>::None,
    ) {
        syscalls.umount(&mountpoint, None);
        return Ok(false);
    };
    wait_mountpoint(&mountpoint, 3, executor).await?;
    Ok(true)
}
pub async fn wait_mountpoint<E: Executor, P: AsRef<Path>>(
    path: P,
    seconds: u32,
    executor: &E,
) -> Result<()> {
    let mut duration = seconds;
    while duration > 0 {
        time::sleep(time::Duration::from_secs(1)).await;
        if let Ok(_) = is_mountpoint(path.as_ref(), executor).await {
            return Ok(());
        }
        duration -= 1;
    }

    bail!("was not mounted in time")
}

#[cfg(test)]
mod test {
    use super::{download_flist, hash_of_flist};
    use std::{ffi::OsStr, fs, path::Path};

    #[tokio::test]
    async fn test_download_flist() {
        fs::create_dir_all("/tmp/flist_test").unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let path = download_flist(url, &Path::new("/tmp/flist_test").to_path_buf())
            .await
            .unwrap();
        println!("{}", &path.display().to_string());
        let filename = hash_of_flist(url).await.unwrap();
        // make sure the downloaded file matches the hash of this flist
        assert_eq!(Some(OsStr::new(&filename)), path.file_name());
        let first_file_created = fs::metadata(&path).unwrap().created().unwrap();
        let path = download_flist(url, &Path::new("/tmp/flist_test").to_path_buf())
            .await
            .unwrap();
        let second_file_created = fs::metadata(&path).unwrap().created().unwrap();
        // make sure the second file is not created because it is the same file
        assert_eq!(first_file_created, second_file_created);
        fs::remove_dir_all("/tmp/flist_test").unwrap();
    }
    #[tokio::test]
    async fn test_hash_of_flist() {
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let hash = hash_of_flist(url).await.unwrap();
        assert_eq!(hash, "efc9269253cb7210d6eded4aa53b7dfc")
    }
}
