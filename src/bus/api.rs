use anyhow::Result;
use rbus::{object, server::Sender};

use crate::bus::types::{
    net::{ExitDevice, IPNet, OptionPublicConfig},
    stats::{Capacity, TimesStat, VirtualMemory},
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
