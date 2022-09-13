use super::{Device, DeviceManager};
use crate::system::{Command, Executor};
use crate::Unit;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub struct LsblkDevice {
    // hold data here
    path: PathBuf,
    size: Unit,
    subsystems: String,
    #[serde(rename = "fstype")]
    filesystem: Option<String>,
    label: Option<String>,
    rota: bool,
}

impl Device for LsblkDevice {
    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn size(&self) -> Unit {
        self.size
    }

    fn subsystems(&self) -> &str {
        self.subsystems.as_str()
    }

    fn filesystem(&self) -> Option<&str> {
        self.filesystem.as_ref().map(|v| v.as_str())
    }

    fn label(&self) -> Option<&str> {
        self.label.as_ref().map(|v| v.as_str())
    }

    fn rota(&self) -> bool {
        self.rota
    }
}

#[derive(Deserialize)]
struct Devices {
    #[serde(rename = "blockdevices")]
    devices: Vec<LsblkDevice>,
}

#[derive(Debug)]
pub struct LsBlk<E>
where
    E: Executor,
{
    exec: E,
}

impl<E> LsBlk<E>
where
    E: Executor,
{
    #[cfg(test)]
    fn new(exec: E) -> Self {
        LsBlk { exec }
    }
}

impl Default for LsBlk<crate::system::System> {
    fn default() -> Self {
        LsBlk {
            exec: crate::system::System,
        }
    }
}

#[async_trait::async_trait]
impl<E> DeviceManager for LsBlk<E>
where
    E: Executor + Send + Sync,
{
    type Device = LsblkDevice;

    async fn devices(&self) -> Result<Vec<Self::Device>> {
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11");

        let output = self.exec.run(&cmd).await?;
        let devices: Devices =
            serde_json::from_slice(&output).context("failed to decode lsblk output")?;

        Ok(devices
            .devices
            .into_iter()
            .filter(|device| device.subsystems() != "block:scsi:usb:pci")
            .collect())
    }

    async fn device<P: AsRef<Path> + Send>(&self, path: P) -> Result<Self::Device> {
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11")
            .arg(path.as_ref());

        let output = self.exec.run(&cmd).await?;
        let devices: Devices =
            serde_json::from_slice(&output).context("failed to decode lsblk output")?;

        let mut devices = devices.devices;
        devices
            .pop()
            .ok_or_else(|| anyhow::anyhow!("device not found"))
    }

    async fn labeled<S: AsRef<str> + Send>(&self, label: S) -> Result<Self::Device> {
        let label = label.as_ref();
        let devices = self.devices().await?;
        for device in devices {
            if let Some(lb) = device.label() {
                if lb == label {
                    return Ok(device);
                }
            }
        }

        anyhow::bail!("device not found");
    }

    async fn shutdown(&self, device: &Self::Device) -> Result<()> {
        let cmd = Command::new("hdparm").arg("-y").arg(device.path());

        self.exec
            .run(&cmd)
            .await
            .context("failed to shutdown device")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{DeviceManager, LsBlk};
    use crate::{storage::device::Device, system::Command};
    use std::path::Path;

    const LSBLK_LIST_VALID: &str = r#"{
        "blockdevices": [
           {"path":"/dev/sda", "name":"/dev/sda", "size":512110190592, "subsystems":"block:scsi:pci", "fstype":"btrfs", "label":"aa8a31a4-cbe8-4615-a6fe-155a9418cd0a", "rota":false},
           {"path":"/dev/sdb", "name":"/dev/sdb", "size":3000592982016, "subsystems":"block:scsi:pci", "fstype":"btrfs", "label":"5ecdbb3c-b687-4048-b505-7a6756c2de76", "rota":true},
           {"path":"/dev/sdc", "name":"/dev/sdc", "size":3000592982016, "subsystems":"block:scsi:pci", "fstype":"btrfs", "label":"fb45d10b-ca67-44c2-9d3a-7c3468dcba5c", "rota":true},
           {"path":"/dev/sdd", "name":"/dev/sdd", "size":3000592982016, "subsystems":"block:scsi:pci", "fstype": null, "label": null, "rota":false},
           {"path":"/dev/sdx", "name":"/dev/sdx", "size":12341245, "subsystems":"block:scsi:usb:pci", "fstype": null, "label": null, "rota":false}
        ]
     }"#;

    const LSBLK_DEVICE_VALID: &str = r#"{
        "blockdevices": [
           {"path":"/dev/sda", "name":"/dev/sda", "size":512110190592, "subsystems":"block:scsi:pci", "fstype":"btrfs", "label":"aa8a31a4-cbe8-4615-a6fe-155a9418cd0a", "rota":false}
        ]
     }"#;

    #[test]
    fn default() {
        // makes sure default implementation works
        let _ = LsBlk::default();
    }

    #[tokio::test]
    async fn lsblk_devices() {
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(LSBLK_LIST_VALID)));

        //mut is only needed for the checkpoint
        let mut lsblk = LsBlk::new(exec);

        let devices = lsblk.devices().await.expect("failed to get devices");
        lsblk.exec.checkpoint();

        let path = Path::new("/dev/sda");

        assert!(devices.len() == 4);
        assert!(devices[0].path() == path);
        assert!(matches!(devices[0].filesystem(), Some(f) if f == "btrfs"));
        assert!(
            matches!(devices[1].label(), Some(l) if l == "5ecdbb3c-b687-4048-b505-7a6756c2de76")
        );
        assert!(matches!(devices[3].filesystem(), None));
        assert!(matches!(devices[3].label(), None));
    }

    #[tokio::test]
    async fn lsblk_device() {
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11")
            .arg("/dev/sda");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(LSBLK_DEVICE_VALID)));

        //mut is only needed for the checkpoint
        let mut lsblk = LsBlk::new(exec);

        let device = lsblk
            .device("/dev/sda")
            .await
            .expect("failed to get device");
        lsblk.exec.checkpoint();

        let path = Path::new("/dev/sda");

        assert!(device.path() == path);
        assert!(matches!(device.filesystem(), Some(f) if f == "btrfs"));
        assert!(matches!(device.label(), Some(l) if l == "aa8a31a4-cbe8-4615-a6fe-155a9418cd0a"));
    }

    #[tokio::test]
    async fn lsblk_device_not_found() {
        use crate::system::Error;

        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11")
            .arg("/dev/sda");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Err(Error::new(32, Some("device not found"))));

        //mut is only needed for the checkpoint
        let mut lsblk = LsBlk::new(exec);

        let device = lsblk.device("/dev/sda").await;

        lsblk.exec.checkpoint();

        assert!(
            matches!(device, Err(err) if err.to_string() == "error-code: 32 - message: device not found")
        );
    }

    #[tokio::test]
    async fn lsblk_device_by_label() {
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(LSBLK_LIST_VALID)));

        //mut is only needed for the checkpoint
        let mut lsblk = LsBlk::new(exec);

        let device = lsblk
            .labeled("5ecdbb3c-b687-4048-b505-7a6756c2de76")
            .await
            .expect("failed to get device");
        lsblk.exec.checkpoint();

        let path = Path::new("/dev/sdb");
        assert!(device.path() == path);
        assert!(matches!(device.filesystem(), Some(f) if f == "btrfs"));
        assert!(matches!(device.label(), Some(l) if l == "5ecdbb3c-b687-4048-b505-7a6756c2de76"));
    }

    #[tokio::test]
    async fn lsblk_shutdown() {
        let mut exec = crate::system::MockExecutor::default();
        let cmd = Command::new("lsblk")
            .arg("--json")
            .arg("-o")
            .arg("PATH,NAME,SIZE,SUBSYSTEMS,FSTYPE,LABEL,ROTA")
            .arg("--bytes")
            .arg("--exclude")
            .arg("1,2,11");

        exec.expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::from(LSBLK_LIST_VALID)));

        //mut is only needed for the checkpoint
        let mut lsblk = LsBlk::new(exec);

        let device = lsblk
            .labeled("5ecdbb3c-b687-4048-b505-7a6756c2de76")
            .await
            .expect("failed to get device");
        lsblk.exec.checkpoint();

        let path = Path::new("/dev/sdb");
        assert!(device.path() == path);

        let cmd = Command::new("hdparm").arg("-y").arg(device.path());

        lsblk
            .exec
            .expect_run()
            .withf(move |arg: &Command| arg == &cmd)
            .times(1)
            .returning(|_: &Command| Ok(Vec::default()));

        lsblk.shutdown(&device).await.unwrap();
        lsblk.exec.checkpoint();
    }
}
