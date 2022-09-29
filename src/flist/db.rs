use anyhow::{bail, Result};
use bytes::Buf;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs;

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
            flist: flist.as_ref().to_path_buf(),
        })
    }
    pub async fn get<T: AsRef<str>>(&self, url: T) -> Result<PathBuf> {
        let url = url.as_ref();
        let hash = self.hash_of_flist(url).await?;
        let path = self.flist.join(&hash.trim());
        match fs::File::open(&path).await {
            Ok(_) => {
                //Flist already exists let's check it's md5
                if self.compare_md5(hash, &path) {
                    return Ok(path);
                } else {
                    self.download_flist(url).await
                }
            }
            Err(error) => match error.kind() {
                io::ErrorKind::NotFound => self.download_flist(url).await,

                _ => bail!(
                    "error reading flist file: {}, error {}",
                    &path.display(),
                    error
                ),
            },
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
    pub async fn download_flist<T: AsRef<str>>(&self, url: T) -> Result<PathBuf> {
        let url = url.as_ref();
        // Flist not found or hash is not correct, let's download
        let mut resp = reqwest::get(url).await?.bytes().await?.reader();
        let mut builder = tempfile::Builder::new();
        let mut file = builder.suffix("_flist_temp").tempfile_in(&self.flist)?;
        io::copy(&mut resp, &mut file)?;
        let tmp_path = file.path();
        let hash =
            checksums::hash_file(Path::new(&tmp_path), checksums::Algorithm::MD5).to_lowercase();
        let path = self.flist.join(&hash);
        if let Some(parent_dir) = path.parent() {
            fs::create_dir_all(parent_dir).await?;
        } else {
            bail!(
                "can not create parent directory of {}",
                path.display().to_string()
            )
        }
        fs::rename(tmp_path, &path).await?;
        Ok(path)
    }

    // get's flist hash from hub
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
}
#[cfg(test)]
mod test {
    use super::MetadataDbMgr;
    use std::ffi::OsStr;
    use tokio::fs;
    #[tokio::test]
    async fn get() {
        let metadata_mgr = MetadataDbMgr::new("/tmp/flist_test").await.unwrap();

        fs::create_dir_all("/tmp/flist_test").await.unwrap();
        let url = "https://hub.grid.tf/ashraf.3bot/ashraffouda-mattermost-latest.flist";
        let path = metadata_mgr.get(url).await.unwrap();
        let filename = metadata_mgr.hash_of_flist(url).await.unwrap();
        // make sure the downloaded file matches the hash of this flist
        assert_eq!(Some(OsStr::new(&filename)), path.file_name());
        let first_file_created = fs::metadata(&path).await.unwrap().created().unwrap();
        let path = metadata_mgr.get(url).await.unwrap();
        let second_file_created = fs::metadata(&path).await.unwrap().created().unwrap();
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
