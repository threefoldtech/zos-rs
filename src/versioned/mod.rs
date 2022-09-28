use anyhow::Context;
use semver::Version;
use std::fs::Permissions;
use std::path::Path;
use std::str::{self, FromStr};
use std::{fmt::Debug, os::unix::prelude::PermissionsExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum allowed version length.
const MAX_VERSION_LENGTH: u8 = 50;

#[derive(Debug, Error)]
pub enum Error {
    /// NotVersioned error is raised if the underlying reader has no version
    #[error("no version information")]
    NotVersioned,

    /// InvalidVersion error is raised if a version is found but it is not valid
    #[error("invalid version: {version}")]
    InvalidVersion { version: String },

    /// VersionLengthExceeded error is raised if [`MAX_VERSION_LENGTH`] is reached before reaching the end of the version's string.
    #[error("max version length is {}", MAX_VERSION_LENGTH)]
    VersionLengthExceeded,

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Creates a new versioned reader from a stream. It fails
/// if the reader can not read the version from the stream.
/// On success, the reader will have a version, and then can be used
/// to load the data.
///
/// The reader should be a type implementing [`AsyncRead`] + [`Unpin`].
///
/// If parsing succeeds, returns `VersionedReader<R>` inside [`Ok`].
///
/// # Errors
/// Returns `Err` if version information is not found or valid, or when there is an io error.
///
/// # Example
/// ```ignore
/// let mut file = tokio::fs::OpenOptions::new()
///     .read(true)
///     .open(path.as_ref())
///     .await?;
/// let mut reader = VersionedReader::new(&mut file).await?;
/// ```
pub async fn reader<R: AsyncRead + Unpin>(mut r: R) -> Result<(Version, R)> {
    let mut double_quotes: u8 = 0;
    let mut version_bytes = Vec::<u8>::new();
    for _ in 0..MAX_VERSION_LENGTH {
        let byte = r.read_u8().await?;
        if double_quotes == 0 && byte != b'\"' {
            return Err(Error::NotVersioned);
        }
        if byte == b'\"' {
            double_quotes += 1;
            if double_quotes == 2 {
                break;
            }
            continue;
        }
        version_bytes.push(byte);
    }
    if double_quotes != 2 {
        return Err(Error::VersionLengthExceeded);
    }
    let version_str = str::from_utf8(&version_bytes)
        .context("failed to convert version information to string")?;
    let version = Version::from_str(version_str).map_err(|_| Error::InvalidVersion {
        version: version_str.into(),
    })?;

    Ok((version, r))
}

/// Reads versioned file's contents.
///
/// If read succeeds, returns a tuple `(semver::Version, Vec<u8>)` containing file version and data inside [`Ok`].
pub async fn read_file<P: AsRef<Path>>(path: P) -> Result<(Version, Vec<u8>)> {
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path.as_ref())
        .await?;
    let (version, reader): (Version, &mut tokio::fs::File) = reader(&mut file).await?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    Ok((version, buf))
}

/// Writes version to a writer implementing [`AsyncWrite`].
pub async fn writer<W: AsyncWrite + Unpin>(mut w: W, version: &Version) -> Result<W> {
    let v_str = serde_json::json!(version.to_string());
    w.write_all(v_str.to_string().as_bytes()).await?;
    Ok(w)
}

/// Writes version and data to a file.
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
    let mut file = writer(file, version).await?;
    file.write_all(data).await?;
    Ok(())
}

#[cfg(test)]

mod test {
    use super::{read_file, write_file, Error};
    use rand::{self, Rng};
    use semver::Version;
    use std::io::Write;
    use std::str::FromStr;
    use std::{fs::Permissions, os::unix::prelude::PermissionsExt};

    #[tokio::test]
    async fn test_max_version_length() {
        let long_version: Vec<u8> = vec![b'a'; 100];
        let reader = format!("\"{}\"data", std::str::from_utf8(&long_version).unwrap());
        let versioned = super::reader(reader.as_bytes()).await;
        assert!(matches!(versioned, Err(Error::VersionLengthExceeded)))
    }

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
        let versioned = super::reader(r#""mario"abcdef"#.as_bytes()).await;

        assert!(matches!(versioned, Err(Error::InvalidVersion{version}) if version == "mario"));
    }

    #[tokio::test]
    async fn test_unversioned() {
        let versioned = super::reader(r#"1.2.3"abcdef"#.as_bytes()).await;

        assert!(matches!(versioned, Err(Error::NotVersioned)));
    }

    #[tokio::test]
    async fn test_read_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, r#""1.5.7-alpha"hello world"#).unwrap();

        let (read_version, read_data) = read_file(file.path()).await.unwrap();

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

        let (read_version, read_data) = read_file(file.path()).await.unwrap();
        assert_eq!(version, read_version);
        assert_eq!(data, read_data);
    }
}
