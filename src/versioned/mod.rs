use anyhow::Context;
use semver::Version;
use std::fs::Permissions;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::str::{self, FromStr};
use std::{fmt::Debug, os::unix::prelude::PermissionsExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub struct VersionedReader<R>
where
    R: AsyncRead + Unpin,
{
    version: Version,
    inner: R,
}

impl<R> VersionedReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn version(&self) -> &Version {
        &self.version
    }
}

impl<R> Deref for VersionedReader<R>
where
    R: AsyncRead + Unpin,
{
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<R> DerefMut for VersionedReader<R>
where
    R: AsyncRead + Unpin,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("no version information")]
    NotVersioned,

    #[error("invalid version: {version}")]
    InvalidVersion { version: String },
    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl<R> VersionedReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn new(mut r: R) -> Result<VersionedReader<R>> {
        let mut double_quotes = false;
        let mut version_bytes = Vec::<u8>::new();
        loop {
            // TODO: add max length for version to prevent from reading whole file before reaching '"'

            let byte = r.read_u8().await?;
            if double_quotes == false && byte != b'\"' {
                return Err(Error::NotVersioned);
            }
            if byte == b'\"' {
                if double_quotes == true {
                    break;
                }
                double_quotes = true;
                continue;
            }
            version_bytes.push(byte);
        }
        let version_str = str::from_utf8(&version_bytes)
            .context("failed to convert version information to string")?;
        let version = Version::from_str(version_str).map_err(|_| Error::InvalidVersion {
            version: version_str.into(),
        })?;

        Ok(VersionedReader::<R> { version, inner: r })
    }
}

pub async fn read_file<P: AsRef<Path>>(path: P) -> Result<(Version, Vec<u8>)> {
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path.as_ref())
        .await?;
    let mut reader = VersionedReader::new(&mut file).await?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok((reader.version, buf))
}

pub async fn new_writer<W: AsyncWrite + Unpin>(mut w: W, version: &Version) -> Result<W> {
    let v_str = serde_json::json!(version.to_string());
    w.write_all(v_str.to_string().as_bytes()).await?;
    Ok(w)
}

pub async fn write_file<P: AsRef<Path>>(
    path: P,
    version: &Version,
    data: &[u8],
    perm: Permissions,
) -> Result<()> {
    let file = tokio::fs::OpenOptions::new()
        .mode(perm.mode())
        .truncate(true)
        .create(true)
        .write(true)
        .open(path.as_ref())
        .await?;
    let mut file = new_writer(file, &version).await?;
    file.write_all(data).await?;
    Ok(())
}

#[cfg(test)]

mod test {
    use super::{write_file, Error, VersionedReader};
    use rand::{self, Rng};
    use semver::Version;
    use std::io::Write;
    use std::str::FromStr;
    use std::{fs::Permissions, os::unix::prelude::PermissionsExt};

    #[tokio::test]
    async fn test_write_file() {
        let data = b"hellowrite";
        let version = Version::from_str("1.5.7-alpha").unwrap();
        let perm = Permissions::from_mode(0400);
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let res = write_file(path, &version, data, perm).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_version() {
        let versioned = VersionedReader::new(r#""mario"abcdef"#.as_bytes()).await;

        assert!(matches!(versioned, Err(Error::InvalidVersion{version}) if version == "mario"));
    }

    #[tokio::test]
    async fn test_unversioned() {
        let versioned = VersionedReader::new(r#"1.2.3"abcdef"#.as_bytes()).await;

        assert!(matches!(versioned, Err(Error::NotVersioned)));
    }

    #[tokio::test]
    async fn test_read_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, r#""1.5.7-alpha"hello world"#).unwrap();

        let (read_version, read_data) = super::read_file(file.path()).await.unwrap();

        let version = Version::from_str("1.5.7-alpha").unwrap();
        assert_eq!(version, read_version);
        assert_eq!(Vec::from("hello world"), read_data);
    }

    #[tokio::test]
    async fn test_write_read_file() {
        let data: Vec<u8> = (0..100)
            .map(|_| rand::thread_rng().gen_range(0..255))
            .collect();
        let version = Version::from_str("1.2.1-beta").unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let res = write_file(file.path(), &version, &data, Permissions::from_mode(0400)).await;
        assert!(res.is_ok());

        let (read_version, read_data) = super::read_file(file.path()).await.unwrap();
        assert_eq!(version, read_version);
        assert_eq!(data, read_data);
    }
}
