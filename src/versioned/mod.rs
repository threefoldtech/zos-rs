use anyhow::{Ok, Result};
use semver::Version;
use std::fs::Permissions;
use std::str::{self, FromStr};
use std::{fmt::Debug, os::unix::prelude::PermissionsExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct VersionedFileReader<R>
where
    R: AsyncRead + Unpin,
{
    pub version: Version,
    pub inner: R,
}

#[derive(Debug, Error)]
enum Error {
    #[error("no verison information")]
    NotVersioned,
}

impl<R> VersionedFileReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn new_reader(mut r: R) -> Result<VersionedFileReader<R>> {
        let mut double_quotes = false;
        let mut version_bytes = Vec::<u8>::new();
        loop {
            // TODO: add max length for version to prevent from reading whole file before reaching '"'
            let byte = r.read_u8().await?;
            if double_quotes == false && byte != b'\"' {
                return Err(anyhow::Error::from(Error::NotVersioned));
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
        let version_str = str::from_utf8(&version_bytes)?;
        let version = Version::from_str(version_str)?;
        Ok(VersionedFileReader::<R> { version, inner: r })
    }

    pub async fn read_file(path: &str) -> Result<(Version, Vec<u8>)> {
        let mut file = tokio::fs::OpenOptions::new().read(true).open(path).await?;
        let reader = VersionedFileReader::new_reader(&mut file).await?;
        let mut buf = Vec::new();
        reader.inner.read_to_end(&mut buf).await?;
        Ok((reader.version, buf))
    }
}

pub async fn new_writer<W: AsyncWrite + Unpin>(w: &mut W, version: &Version) -> Result<()> {
    let v_str = serde_json::json!(version.to_string());
    w.write_all(v_str.to_string().as_bytes()).await?;
    Ok(())
}

pub async fn write_file(
    path: &str,
    version: &Version,
    data: &[u8],
    perm: Permissions,
) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .mode(perm.mode())
        .truncate(true)
        .create(true)
        .write(true)
        .open(path)
        .await?;
    new_writer(&mut file, &version).await?;
    file.write_all(data).await?;
    Ok(())
}

#[cfg(test)]

mod test {
    use crate::versioned::write_file;
    use crate::versioned::VersionedFileReader;
    use rand::{self, Rng};
    use semver::Version;
    use std::io::Write;
    use std::str::FromStr;
    use std::{fs::Permissions, os::unix::prelude::PermissionsExt};
    use tokio::fs::File;

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
    async fn test_read_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_fmt(format_args!("\"1.5.7-alpha\"hello world"))
            .unwrap();
        let path = file.path().to_str().unwrap();

        let version = Version::from_str("1.5.7-alpha").unwrap();
        let data: Vec<u8> = Vec::from("hello world");
        let (read_version, read_data) = match VersionedFileReader::<File>::read_file(path).await {
            Ok((version, data)) => (version, data),
            Err(err) => panic!("{}", err.to_string()),
        };
        assert_eq!(version, read_version);
        assert_eq!(data, read_data);
    }

    #[tokio::test]
    async fn test_write_read_file() {
        let data: Vec<u8> = (0..100)
            .map(|_| rand::thread_rng().gen_range(0..255))
            .collect();
        let version = Version::from_str("1.2.1-beta").unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let res = write_file(path, &version, &data, Permissions::from_mode(0400)).await;
        assert!(res.is_ok());

        let (read_version, read_data) = match VersionedFileReader::<File>::read_file(path).await {
            Ok((version, data)) => (version, data),
            Err(err) => panic!("{}", err.to_string()),
        };
        assert_eq!(version, read_version);
        assert_eq!(data, read_data);
    }
}
