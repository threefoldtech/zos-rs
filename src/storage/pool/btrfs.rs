use super::{Error, Pool, Result, Volume};
use crate::storage::device::Device;
use crate::system::{Command, Executor};
use crate::Unit;
use anyhow::Context;
use std::path::{Path, PathBuf};

/// root mount path
const MNT: &str = "/mnt";

pub struct BtrfsVolume {
    path: PathBuf,
}

#[async_trait::async_trait]
impl Volume for BtrfsVolume {
    fn id(&self) -> u64 {
        unimplemented!()
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn name(&self) -> &str {
        self.path
            .file_name()
            .map(|s| s.to_str().unwrap())
            .unwrap_or("unknown")
    }

    async fn limit(&self, size: Unit) -> Result<()> {
        Err(Error::Unsupported)
    }

    async fn usage(&self) -> Result<Unit> {
        unimplemented!()
    }
}

pub struct BtrfsPool<D>
where
    D: Device,
{
    device: D,
    path: PathBuf,
}

#[async_trait::async_trait]
impl<D> Volume for BtrfsPool<D>
where
    D: Device + Send + Sync,
{
    fn id(&self) -> u64 {
        0
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn name(&self) -> &str {
        self.device.label().unwrap_or("unknown")
    }

    async fn limit(&self, size: Unit) -> Result<()> {
        Err(Error::Unsupported)
    }

    async fn usage(&self) -> Result<Unit> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl<D> Pool for BtrfsPool<D>
where
    D: Device + Send + Sync,
{
    type Volume = BtrfsVolume;

    async fn mount(&self) -> Result<PathBuf> {
        let device = self.device.path().to_str().ok_or(Error::InvalidDevice {
            device: self.device.path().into(),
        })?;

        let mnt = crate::storage::mountinfo(&device)
            .await?
            .into_iter()
            .filter(|m| matches!(m.option("subvol"), Some(Some(target)) if target == "/"))
            .next();

        if let Some(mnt) = mnt {
            return Ok(mnt.target);
        }

        //self.device.label()
        //let path = PathBuf::from(MNT).join(self.device.label());

        unimplemented!()
    }

    async fn unmount(&self) -> Result<()> {
        unimplemented!()
    }

    async fn volumes(&self) -> Result<Vec<Self::Volume>> {
        unimplemented!()
    }

    async fn volume<S: AsRef<str> + Send>(&self, name: S) -> Result<Self::Volume> {
        unimplemented!()
    }

    async fn delete<S: AsRef<str> + Send>(&self, name: S) -> Result<()> {
        unimplemented!()
    }
}

struct QGroupInfo {
    id: String,
    rfer: Unit,
    excl: Unit,
    max_rfer: Option<Unit>,
    max_excl: Option<Unit>,
}

struct VolumeInfo {
    id: u64,
    name: String,
}

struct BtrfsUtils<E: Executor> {
    exec: E,
}

impl<E: Executor> BtrfsUtils<E> {
    fn new(exec: E) -> Self {
        Self { exec }
    }

    async fn volume_create<P: AsRef<Path>, S: AsRef<str>>(&self, root: P, name: S) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("create")
            .arg(name.as_ref())
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn volume_delete<P: AsRef<Path>, S: AsRef<str>>(&self, root: P, name: S) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("delete")
            .arg(name.as_ref())
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn volume_id<P: AsRef<Path>>(&self, volume: P) -> Result<u64> {
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("show")
            .arg(volume.as_ref());

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_volume_info(&output)?)
    }

    async fn volume_list<P: AsRef<Path>>(&self, root: P) -> Result<Vec<VolumeInfo>> {
        let cmd = Command::new("btrfs")
            .arg("subvolume")
            .arg("list")
            .arg("-o")
            .arg(root.as_ref());

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_volumes(&output)?)
    }

    async fn qgroup_enable<P: AsRef<Path>>(&self, root: P) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("quota")
            .arg("enable")
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn qgroup_limit<P: AsRef<Path>>(&self, volume: P, size: Option<Unit>) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("limit")
            .arg(match size {
                Some(limit) => format!("{}", limit),
                None => "none".into(),
            })
            .arg(volume.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn qgroup_delete<P: AsRef<Path>>(&self, root: P, volume_id: u64) -> Result<()> {
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("destroy")
            .arg(format!("0/{}", volume_id))
            .arg(root.as_ref());

        self.exec.run(&cmd).await?;
        Ok(())
    }

    async fn groupl_list<P: AsRef<Path>>(&self, root: P) -> Result<Vec<QGroupInfo>> {
        // qgroup show -re --raw .
        let cmd = Command::new("btrfs")
            .arg("qgroup")
            .arg("show")
            .arg("-re")
            .arg("--raw")
            .arg(root.as_ref());

        let output = self.exec.run(&cmd).await?;
        Ok(self.parse_qgroup(&output)?)
    }

    fn parse_volume_info(&self, data: &[u8]) -> anyhow::Result<u64> {
        //todo: probably better to use regex or just scan
        //the string until the id is found than allocating strings
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines();
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.splitn(2, ":").collect();
            if parts.len() != 2 {
                continue;
            }
            if parts[0].trim() == "Subvolume ID" {
                return Ok(parts[1].trim().parse()?);
            }
        }

        anyhow::bail!("failed to extract subvolume id")
    }

    fn parse_qgroup(&self, data: &[u8]) -> anyhow::Result<Vec<QGroupInfo>> {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines().skip(2);
        let mut groups = vec![];
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 5 {
                continue;
            }
            let group = QGroupInfo {
                id: parts[0].into(),
                rfer: parts[1].parse()?,
                excl: parts[2].parse()?,
                max_rfer: if parts[3] == "none" {
                    None
                } else {
                    Some(parts[3].parse()?)
                },
                max_excl: if parts[4] == "none" {
                    None
                } else {
                    Some(parts[4].parse()?)
                },
            };
            groups.push(group);
        }

        Ok(groups)
    }

    fn parse_volumes(&self, data: &[u8]) -> anyhow::Result<Vec<VolumeInfo>> {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(data);
        let mut lines = reader.lines();
        let mut volumes = vec![];
        while let Some(line) = lines.next() {
            let line = line?;
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 9 {
                continue;
            }
            let group = VolumeInfo {
                id: parts[1].parse()?,
                name: parts[8].into(),
            };
            volumes.push(group);
        }

        Ok(volumes)
    }
}

impl Default for BtrfsUtils<crate::system::System> {
    fn default() -> Self {
        BtrfsUtils {
            exec: crate::system::System,
        }
    }
}

#[cfg(test)]
mod test {
    use super::BtrfsUtils;

    #[test]
    fn utils_vol_info_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"b623b3b159fa02652bb21c695a157b4d
        Name: 			b623b3b159fa02652bb21c695a157b4d
        UUID: 			abf4240e-6402-9947-963e-63db1a7f5582
        Parent UUID: 		-
        Received UUID: 		-
        Creation time: 		2022-02-03 12:58:32 +0000
        Subvolume ID: 		1740
        Generation: 		33008608
        Gen at creation: 	199304
        Parent ID: 		5
        Top level ID: 		5
        Flags: 			-
        Snapshot(s):
        "#;

        let id = utils.parse_volume_info(DATA.as_bytes()).unwrap();
        assert_eq!(id, 1740);
    }

    #[test]
    fn utils_qgroup_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"qgroupid         rfer         excl     max_rfer     max_excl
--------         ----         ----     --------     --------
0/256      1732771840   1732771840 107374182400         none
0/262     60463501312  60463501312         none         none
0/1596          16384        16384     10485760         none
0/1737          16384        16384     10485760         none
0/1740          16384        16384     10485760         none
0/4301      524271616    524271616    524288000         none
0/4303      524271616    524271616    524288000         none
0/4849      106655744    106655744   2147483648         none
0/7437        6471680      6471680  10737418240         none
0/7438     1525182464   1525182464   2147483648         none
        "#;

        let groups = utils.parse_qgroup(DATA.as_bytes()).unwrap();
        assert_eq!(groups.len(), 10);
        let group0 = &groups[0];
        let group1 = &groups[1];

        assert_eq!(group0.id, "0/256");
        assert_eq!(group0.rfer, 1732771840);
        assert_eq!(group0.excl, 1732771840);
        assert_eq!(group0.max_rfer, Some(107374182400));
        assert_eq!(group0.max_excl, None);

        assert_eq!(group1.id, "0/262");
        assert_eq!(group1.rfer, 60463501312);
        assert_eq!(group1.excl, 60463501312);
        assert_eq!(group1.max_rfer, None);
        assert_eq!(group1.max_excl, None);
    }

    #[test]
    fn utils_volumes_parse() {
        let utils = BtrfsUtils::default();
        const DATA: &str = r#"ID 256 gen 33152047 top level 5 path zos-cache
ID 262 gen 33152049 top level 5 path vdisks
ID 1596 gen 117776 top level 5 path bfb95cf4f1b6245f56a7fb7a86bd1e0d
ID 1737 gen 156823 top level 5 path 794e0004fd49a7300d612dcbba10279f
ID 1740 gen 33008608 top level 5 path b623b3b159fa02652bb21c695a157b4d
ID 4301 gen 5392957 top level 5 path rootfs:433-3764-mr
ID 4303 gen 32919873 top level 5 path rootfs:433-3764-w1
ID 4849 gen 33152049 top level 5 path rootfs:288-5475-owncloud_samehabouelsaad
ID 7437 gen 33152049 top level 5 path 647-10988-qsfs
ID 7438 gen 33152049 top level 5 path rootfs:647-10988-vm
        "#;

        let vols = utils.parse_volumes(DATA.as_bytes()).unwrap();
        assert_eq!(vols.len(), 10);
        let vol0 = &vols[0];
        let vol1 = &vols[1];

        assert_eq!(vol0.id, 256);
        assert_eq!(vol0.name, "zos-cache");

        assert_eq!(vol1.id, 262);
        assert_eq!(vol1.name, "vdisks");
    }
}
