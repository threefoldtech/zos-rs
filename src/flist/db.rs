use anyhow::{bail, Result};
use futures::StreamExt;
use md5::{Digest, Md5};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub struct MetadataDbMgr {
    // root directory where all
    // the working file of the module will be located

    // underneath are the path for each
    // sub folder used by the flist module
    flist: PathBuf,
}

impl MetadataDbMgr {
    pub async fn new<P: AsRef<Path>>(flist: P) -> Result<Self> {
        Ok(Self {
            flist: flist.as_ref().into(),
        })
    }

    pub async fn get<T: AsRef<str>>(&self, url: T) -> Result<(String, PathBuf)> {
        let url = url.as_ref();
        let hash = self.hash_of_flist(url).await?;
        let path = self.flist.join(&hash);

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        if file.metadata().await?.len() == 0 || !self.compare_md5(&hash, &mut file).await {
            self.download_flist(url, &hash, &mut file).await
        } else {
            Ok((hash, path))
        }
    }

    async fn compare_md5<T: AsRef<str>>(&self, hash: T, file: &mut File) -> bool {
        let mut buffer = [0; 4096];

        let mut hasher = Md5::new();
        loop {
            if let Ok(n) = file.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            } else {
                return false;
            }
        }
        let result = hasher.finalize();
        let calculated_hash = base16ct::lower::encode_string(&result);
        calculated_hash == hash.as_ref()
    }

    // downloadFlist downloads an flits from a URL
    // if the flist location also provide and md5 hash of the flist
    // this function will use it to avoid downloading an flist that is
    // already present locally
    async fn download_flist<T: AsRef<str>, H: AsRef<str>>(
        &self,
        url: T,
        hash_from_url: H,
        file: &mut File,
    ) -> Result<(String, PathBuf)> {
        let url = url.as_ref();
        // Flist not found or hash is not correct, let's download
        let mut resp = reqwest::get(url).await?.bytes_stream();
        let mut hasher = Md5::new();
        while let Some(Ok(v)) = resp.next().await {
            file.write_all(&v).await?;
            hasher.update(&v);
        }
        let result = hasher.finalize();
        let hash = base16ct::lower::encode_string(&result);
        if hash != hash_from_url.as_ref() {
            bail!("failed to download flist, incompatible hash")
        }
        let path = self.flist.join(&hash);
        Ok((hash, path))
    }

    // get's flist hash from hub
    async fn hash_of_flist<T: AsRef<str>>(&self, url: T) -> Result<String> {
        let md5_url = format!("{}.md5", url.as_ref());
        let res = reqwest::get(md5_url)
            .await?
            .text()
            .await?
            .trim()
            .to_string();
        Ok(res)
    }
}
#[cfg(test)]
mod test {
    use super::MetadataDbMgr;
    use std::ffi::OsStr;
    use tokio::fs;
    #[tokio::test]
    async fn test_get() {
        let metadata_mgr = MetadataDbMgr::new("/tmp/flist_test").await.unwrap();

        fs::create_dir_all("/tmp/flist_test").await.unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let (_, path) = metadata_mgr.get(url).await.unwrap();
        let filename = metadata_mgr.hash_of_flist(url).await.unwrap();
        // make sure the downloaded file matches the hash of this flist
        assert_eq!(Some(OsStr::new(&filename)), path.file_name());
        let first_file_created = fs::metadata(&path).await.unwrap().modified().unwrap();

        // get the flist again shouldn't download it from the beggining
        let (_, path) = metadata_mgr.get(url).await.unwrap();
        let second_file_created = fs::metadata(&path).await.unwrap().modified().unwrap();
        // make sure the second file is not created because it is the same file
        assert_eq!(first_file_created, second_file_created);
        fs::remove_dir_all("/tmp/flist_test").await.unwrap();
    }
    #[tokio::test]
    async fn test_hash_of_flist() {
        let metadata_mgr = MetadataDbMgr::new("/tmp/flist_test").await.unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let hash = metadata_mgr.hash_of_flist(url).await.unwrap();
        assert_eq!(hash, "efc9269253cb7210d6eded4aa53b7dfc")
    }
}
