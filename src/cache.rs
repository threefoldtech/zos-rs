use crate::system::{Syscalls, System};
use crate::Unit;
use anyhow::{Context, Result};
use nix::mount::MsFlags;
use std::ffi::OsStr;
use std::fmt::Display;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::fs;

const VOLATILE_ROOT: &str = "/var/run/cache";

// creates a volatile directory (under /var/run/cache)
// with
pub async fn volatile<S: AsRef<str>>(name: S, size: Unit) -> Result<PathBuf> {
    let path = Path::new(VOLATILE_ROOT).join(name.as_ref());
    let info = crate::storage::mountpoint(&path).await?;
    if info.is_some() {
        return Ok(path);
    }

    fs::create_dir_all(&path)
        .await
        .with_context(|| format!("failed to create directory: {:?}", path))?;

    System.mount(
        Option::<&str>::None,
        &path,
        Some("tmpfs"),
        MsFlags::empty(),
        Some(format!("size={}", size)),
    )?;

    Ok(path)
}

pub struct Store<T> {
    path: PathBuf,
    phantom: PhantomData<T>,
}

impl<T> Store<T> {
    #[cfg(not(test))]
    /// create a new instance of cache
    pub async fn new<S: AsRef<str>>(name: S, size: Unit) -> Result<Self> {
        let path = volatile(name, size).await?;

        Ok(Store {
            path: path,
            phantom: PhantomData::default(),
        })
    }

    #[cfg(test)]
    /// this version of the cache doesn't use mount and also automatically purge the cache each time
    /// it's created
    pub async fn new<S: AsRef<str>>(name: S, _size: Unit) -> Result<Self> {
        let path = std::env::temp_dir().join(name.as_ref());
        Ok(Store {
            path,
            phantom: PhantomData::default(),
        })
    }
}

impl<T: Display> Store<T> {
    pub async fn set<S: AsRef<OsStr>>(&self, key: S, data: &T) -> Result<()> {
        if cfg!(test) {
            return Ok(());
        }
        let path = self.path.join(key.as_ref());
        tokio::fs::write(&path, data.to_string())
            .await
            .with_context(|| format!("failed to write file: {:?}", path))?;
        Ok(())
    }
}

impl<T: FromStr> Store<T> {
    pub async fn get<S: AsRef<OsStr>>(&self, key: S) -> Result<Option<T>> {
        // cache is not enabled during testing.
        if cfg!(test) {
            return Ok(None);
        }
        let path = self.path.join(key.as_ref());
        let data = match tokio::fs::read(&path).await {
            Ok(data) => data,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => anyhow::bail!(err),
        };

        let st = String::from_utf8(data).context("invalid file content not valid utf8")?;

        let t: T = match st.parse() {
            Ok(t) => t,
            Err(_) => anyhow::bail!("failed to file content: {:?}", path),
        };

        Ok(Some(t))
    }
}

// todo! add tests for cache
