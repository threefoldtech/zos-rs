use crate::system::{Command, Executor, Syscalls};
use anyhow::{bail, Result};
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{collections::HashMap, fs, path::Path};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct G8ufsInfo {
    pub pid: i64,
}

struct OverlayInfo {
    lower_dir: String,
    upper_dir: String,
    work_dir: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FsType {
    G8UFS,
    Overlay,
    EXT4,
    Other,
}
impl AsRef<str> for FsType {
    fn as_ref(&self) -> &str {
        match self {
            FsType::G8UFS => "fuse.g8ufs",
            FsType::Overlay => "overlay",
            FsType::EXT4 => "ext4",
            FsType::Other => "other",
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
        for part in self.options.split(',') {
            let kv: Vec<&str> = part.splitn(2, '=').collect();
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
        Ok(g8ufsinfo)
    } else if info.fstype == FsType::Overlay.as_ref() {
        let overlay = info.as_overlay()?;
        resolve(&overlay.lower_dir, executor).await
    } else {
        bail!("invalid mount fs type {}", info.fstype)
    }
}

pub async fn clean_unused_mounts<P, E, S>(
    root: P,
    ro: P,
    mountpoint: P,
    executor: &E,
    syscalls: &S,
) -> Result<()>
where
    P: AsRef<Path>,
    E: Executor,
    S: Syscalls,
{
    let all = list(executor).await?;
    let mut ro_targets = HashMap::new();
    // Get all flists managed by flist Daemony
    let all_under_root = all
        .clone()
        .into_iter()
        .filter(|mnt_info| Path::new(&mnt_info.target).starts_with(&root));
    // Get all under ro dir
    let ros = all_under_root.filter(|mnt_info| {
        Path::new(&mnt_info.target).parent() == Some(ro.as_ref())
            && mnt_info.fstype == FsType::G8UFS.as_ref()
    });
    for mnt_info in ros {
        let g8ufs = mnt_info.as_g8ufs()?;
        ro_targets.insert(g8ufs.pid, mnt_info);
    }

    let all_under_mountpoints = all
        .clone()
        .into_iter()
        .filter(|mnt_info| Path::new(&mnt_info.target).parent() == Some(mountpoint.as_ref()));

    for mnt_info in all_under_mountpoints {
        let pid: i64;
        if mnt_info.fstype == FsType::G8UFS.as_ref() {
            pid = mnt_info.as_g8ufs()?.pid
        } else if mnt_info.fstype == FsType::Overlay.as_ref() {
            let lower_dir_path = mnt_info.as_overlay()?.lower_dir;
            let all_matching_overlay: Vec<MountInfo> = all
                .clone()
                .into_iter()
                .filter(|mnt| Path::new(&lower_dir_path) == Path::new(&mnt.target))
                .collect();
            if all_matching_overlay.len() == 0 {
                continue;
            }
            pid = all_matching_overlay[0].as_g8ufs()?.pid
        } else {
            continue;
        }
        ro_targets.remove(&pid);
    }
    for (_, mount) in ro_targets.into_iter() {
        log::debug!("Cleaning up mount {:#?}", mount);
        if let Err(err) = syscalls.umount(&mount.target, None) {
            log::debug!("failed to unmount {:#?} Error: {}", mount, err);
            continue;
        }
        if let Err(err) = fs::remove_dir_all(&mount.target) {
            log::debug!(
                "failed to remove dir {} for mount {:#?} Error: {}",
                &mount.target,
                mount,
                err
            );
        }
    }
    Ok(())
}

pub async fn list<E: Executor>(executor: &E) -> Result<Vec<MountInfo>> {
    let cmd = Command::new("findmnt").arg("-J").arg("-l");
    let output = executor.run(&cmd).await?;
    let filesysteminfo: FileSystemInfo = serde_json::from_slice(&output)?;
    Ok(filesysteminfo.filesystems)
}

#[cfg(test)]
mod test {
    use super::{list, resolve, FsType, MountInfo};
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
    #[tokio::test]
    async fn test_mounts() {
        const MOUNT_INFO: &str = r#"{
        "filesystems": [
            {"target":"/var/run/cache/storage", "source":"none", "fstype":"tmpfs", "options":"rw,relatime,size=1024k"},
            {"target":"/mnt/5664e665-c29f-48f0-bab3-30a739574433", "source":"/dev/vda", "fstype":"btrfs", "options":"rw,relatime,space_cache,subvolid=5,subvol=/"},
            {"target":"/var/cache", "source":"/dev/vda[/zos-cache]", "fstype":"btrfs", "options":"rw,relatime,space_cache,subvolid=257,subvol=/zos-cache"},
            {"target":"/var/run/netns/ndmz", "source":"nsfs[net:[4026532379]]", "fstype":"nsfs", "options":"rw"},
            {"target":"/var/cache/modules/flistd/ro/bc8d1f6fc1d6c33137466d3a69b68a94", "source":"1272", "fstype":"fuse.g8ufs", "options":"ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other"},
            {"target":"/var/cache/modules/flistd/mountpoint/traefik:bc8d1f6fc1d6c33137466d3a69b68a94", "source":"1272", "fstype":"fuse.g8ufs", "options":"ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other"},
            {"target":"/var/run/cache/networkd", "source":"none", "fstype":"tmpfs", "options":"rw,relatime,size=51200k"},
            {"target":"/var/run/cache/vmd", "source":"none", "fstype":"tmpfs", "options":"rw,relatime,size=51200k"}
        ]
            }"#;
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("findmnt").arg("-J").arg("-l");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(MOUNT_INFO)));

        let res: Vec<MountInfo> = list(&exec)
            .await
            .unwrap()
            .into_iter()
            .filter(|mnt| mnt.fstype == FsType::G8UFS.as_ref())
            .collect();

        assert_eq!(res.len(), 2);
    }
}
