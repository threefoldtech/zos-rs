use crate::{
    storage::Mount,
    system::{Command, Executor},
};
use anyhow::{bail, Result};
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct G8ufsInfo {
    pub pid: i64,
}

struct OverlayInfo {
    lower_dir: String,
    upper_dir: String,
    work_dir: String,
}
enum ResolveInfo {
    G8ufsInfo,
    OverlayInfo,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FsType {
    G8UFS,
    OVERLAY,
    EXT4,
    Other,
}
impl AsRef<str> for FsType {
    fn as_ref(&self) -> &str {
        match self {
            FsType::G8UFS => "fuse.g8ufs",
            FsType::OVERLAY => "overlay",
            FsType::EXT4 => "ext4",
            FsType::Other => "other",
        }
    }
}

impl FromStr for FsType {
    type Err = &'static str;

    fn from_str(input: &str) -> std::result::Result<FsType, Self::Err> {
        match input {
            "fuse.g8ufs" => Ok(FsType::G8UFS),
            "overlay" => Ok(FsType::OVERLAY),
            "ext4" => Ok(FsType::EXT4),
            _ => Ok(FsType::Other),
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MountInfo {
    target: String,
    source: String,
    fstype: String,
    options: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileSystemInfo {
    filesystems: Vec<MountInfo>,
}
impl MountInfo {
    fn as_g8ufs(&self) -> Result<G8ufsInfo> {
        let pid: i64 = self.source.parse::<i64>()?;
        Ok(G8ufsInfo { pid })
    }
    fn as_overlay(&self) -> Result<OverlayInfo> {
        let mut lower_dir: &str = "";
        let mut upper_dir: &str = "";
        let mut work_dir: &str = "";
        for part in self.options.split(",") {
            let kv: Vec<&str> = part.splitn(2, "=").collect();
            if kv.len() != 2 {
                continue;
            }
            match kv[0] {
                "lowerdir" => lower_dir = kv[1],
                "upperdir" => upper_dir = kv[1],
                "workdir" => work_dir = kv[1],
                _ => continue,
            };
        }
        if lower_dir.is_empty() || upper_dir.is_empty() || work_dir.is_empty() {
            bail!("Bad overlay options")
        }
        Ok(OverlayInfo {
            lower_dir: lower_dir.into(),
            upper_dir: upper_dir.into(),
            work_dir: work_dir.into(),
        })
    }
}

pub async fn get_mount<S: AsRef<Path>, E: Executor>(path: S, executor: &E) -> Result<MountInfo> {
    let cmd = Command::new("findmnt").arg("-J").arg(path.as_ref());
    let output = executor.run(&cmd).await?;
    // println!("{:?}", String::from_utf8_lossy(&output));
    let filesysteminfo: FileSystemInfo = serde_json::from_slice(&output)?;
    if filesysteminfo.filesystems.len() != 1 {
        bail!("Invalid number of filesystems");
    }
    let mountinfo = filesysteminfo.filesystems[0].clone();
    Ok(mountinfo)
}

#[async_recursion]
pub async fn resolve<S: AsRef<Path> + Send, E: Executor + Send + Sync>(
    path: S,
    executor: &E,
) -> Result<G8ufsInfo> {
    let info = get_mount(path, executor).await?;
    if info.fstype == FsType::G8UFS.as_ref() {
        let g8ufsinfo = info.as_g8ufs()?;
        return Ok(g8ufsinfo);
    } else if info.fstype == FsType::OVERLAY.as_ref() {
        let overlay = info.as_overlay()?;
        return Ok(resolve(&overlay.lower_dir, executor).await?);
    } else {
        bail!("invalid mount fs type {}", info.fstype)
    }
}

#[cfg(test)]
mod test {
    use super::resolve;
    use crate::system::Command;

    #[tokio::test]
    async fn test_resolve() {
        const MOUNT_INFO: &str = r#"{
        "filesystems": [
            {"target":"/var/cache/modules/flistd/mountpoint/traefik:bc8d1f6fc1d6c33137466d3a69b68a94", "source":"1272", "fstype":"fuse.g8ufs", "options":"ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other"}
        ]
        }
        "#;
        let path = "/var/cache/modules/flistd/mountpoint/traefik:bc8d1f6fc1d6c33137466d3a69b68a94";
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("findmnt").arg("-J").arg(path);

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(MOUNT_INFO)));

        let g8ufsinfo = resolve(&path, &exec).await.unwrap();
        assert_eq!(g8ufsinfo.pid, 1272);
    }
}
