use anyhow::Result;
use rbus::{object, server::Sender};

use crate::bus::types::{
    net::{ExitDevice, IPNet, OptionPublicConfig},
    stats::{Capacity, TimesStat, VirtualMemory},
    storage,
    version::Version,
};

type FarmID = u32;

#[object(module = "identityd", name = "manager", version = "0.0.1")]
pub trait IdentityManager {
    #[rename("FarmID")]
    fn farm_id(&self) -> Result<FarmID>;
    #[rename("Farm")]
    fn farm(&self) -> Result<String>;
}

#[object(module = "identityd", name = "monitor", version = "0.0.1")]
#[async_trait::async_trait]
pub trait VersionMonitor {
    #[rename("Version")]
    #[stream]
    async fn version(&self, rec: Sender<Version>);
}

#[object(module = "registrar", name = "registrar", version = "0.0.1")]
pub trait Registrar {
    #[rename("NodeID")]
    fn node_id(&self) -> Result<u32>;
}

#[object(module = "provision", name = "statistics", version = "0.0.1")]
#[async_trait::async_trait]
pub trait Statistics {
    #[rename("ReservedStream")]
    #[stream]
    async fn reserved(&self, rec: Sender<Capacity>);
}

#[object(module = "node", name = "system", version = "0.0.1")]
#[async_trait::async_trait]
pub trait SystemMonitor {
    #[rename("CPU")]
    #[stream]
    async fn cpu(&self, rec: Sender<TimesStat>);
    #[rename("Memory")]
    #[stream]
    async fn memory(&self, rec: Sender<VirtualMemory>);
}

pub type NetlinkAddresses = Vec<IPNet>;
#[object(module = "network", name = "network", version = "0.0.1")]
#[async_trait::async_trait]
pub trait Networker {
    #[rename("ZOSAddresses")]
    #[stream]
    async fn zos_addresses(&self, rec: Sender<NetlinkAddresses>);

    #[rename("YggAddresses")]
    #[stream]
    async fn ygg_addresses(&self, rec: Sender<NetlinkAddresses>);

    #[rename("DMZAddresses")]
    #[stream]
    async fn dmz_addresses(&self, rec: Sender<NetlinkAddresses>);

    #[rename("PublicAddresses")]
    #[stream]
    async fn public_addresses(&self, rec: Sender<OptionPublicConfig>);

    #[rename("GetPublicExitDevice")]
    fn get_public_exit_device(&self) -> Result<ExitDevice>;
}

#[object(module = "flist", name = "flist", version = "0.0.1")]
#[async_trait::async_trait]
pub trait Flister {
    /**
     * // Mount mounts an flist located at url using the 0-db located at storage
    // in a RO mode. note that there is no way u can unmount a ro flist because
    // it can be shared by many users, it's then up to system to decide if the
    // mount is not needed anymore and clean it up
    Mount(name, url string, opt MountOptions) (path string, err error)

    // UpdateMountSize change the mount size
    UpdateMountSize(name string, limit gridtypes.Unit) (path string, err error)

    // Umount a RW mount. this only unmounts the RW layer and remove the assigned
    // volume.
    Unmount(name string) error

    // HashFromRootPath returns flist hash from a running g8ufs mounted with NamedMount
    HashFromRootPath(name string) (string, error)

    // FlistHash returns md5 of flist if available (requesting the hub)
    FlistHash(url string) (string, error)

    Exists(name string) (bool, error)
     */

    #[rename("Mount")]
    async fn mount(name: String, url: String, options: storage::MountMode) -> Result<String>;

    //todo: add the remaining methods
}
