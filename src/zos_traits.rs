use std::net::{Ipv4Addr, Ipv6Addr};

use anyhow::Result;
use ipnet::IpNet;
use psutil::memory::VirtualMemory;
use rbus::{object, server::Sender};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

type FarmID = u32;

#[object(name = "manager", version = "0.0.1")]
pub trait IdentityManager {
    #[rename("FarmID")]
    fn farm_id(&self) -> Result<FarmID>;
    #[rename("Farm")]
    fn farm(&self) -> Result<String>;
}

#[object(name = "registrar", version = "0.0.1")]
pub trait Registrar {
    #[rename("NodeID")]
    fn node_id(&self) -> Result<u32>;
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRVersion {
    #[serde(rename = "VersionStr")]
    pub version_str: String,
    #[serde(rename = "VersionNum")]
    pub version_num: u64,
    #[serde(rename = "IsNum")]
    pub is_num: bool,
}
impl PRVersion {
    fn to_string(&self) -> String {
        if self.is_num {
            format!("{}", self.version_num)
        } else {
            format!("{}", self.version_str)
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    #[serde(rename = "Major")]
    pub major: u64,
    #[serde(rename = "Minor")]
    pub minor: u64,
    #[serde(rename = "Patch")]
    pub patch: u64,
    #[serde(rename = "Pre")]
    pub pre: Option<Vec<PRVersion>>,
    #[serde(rename = "Build")]
    pub build: Option<Vec<String>>, //No Precendence
}
impl Version {
    pub fn to_string(&self) -> String {
        let mut version_str = format!("{}.{}.{}", self.major, self.minor, self.patch);
        if let Some(pre) = &self.pre {
            version_str = format!("{}_{}", version_str, pre[0].to_string());
            for pre_version in pre[1..].iter() {
                version_str = format!("{}.{}", version_str, pre_version.to_string())
            }
        }
        if let Some(build) = &self.build {
            version_str = format!("{}+{}", version_str, build[0].to_string());
            for build_item in build[1..].iter() {
                version_str = format!("{}.{}", version_str, build_item)
            }
        }
        version_str
    }
}
#[object(name = "monitor", version = "0.0.1")]
#[async_trait::async_trait]
pub trait VersionMonitor {
    #[rename("Version")]
    #[stream]
    async fn version(&self, rec: Sender<Version>);
}

pub type Unit = u64;
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Capacity {
    #[serde(rename = "CRU")]
    pub cru: u64,
    #[serde(rename = "SRU")]
    pub sru: Unit,
    #[serde(rename = "HRU")]
    pub hru: Unit,
    #[serde(rename = "MRU")]
    pub mru: Unit,
    #[serde(rename = "IPV4U")]
    pub ipv4u: u64,
}

#[object(name = "statistics", version = "0.0.1")]
#[async_trait::async_trait]
pub trait Statistics {
    #[rename("ReservedStream")]
    #[stream]
    async fn reserved_stream(&self, rec: Sender<Capacity>);
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ZOSVirtualMemory {
    #[serde(rename = "Total")]
    pub total: u64,
    #[serde(rename = "Available")]
    pub available: u64,
    #[serde(rename = "Used")]
    pub used: u64,
    #[serde(rename = "UsedPercent")]
    pub used_percent: f64,
}
impl From<VirtualMemory> for ZOSVirtualMemory {
    fn from(mem: VirtualMemory) -> Self {
        ZOSVirtualMemory {
            total: mem.total(),
            available: mem.available(),
            used: mem.used(),
            used_percent: mem.percent() as f64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZOSTimesStat {
    #[serde(rename = "Percent")]
    pub percent: f64,
}
#[object(name = "system", version = "0.0.1")]
#[async_trait::async_trait]
pub trait SystemMonitor {
    #[rename("CPU")]
    #[stream]
    async fn cpu(&self, rec: Sender<ZOSTimesStat>);
    #[rename("Memory")]
    #[stream]
    async fn memory(&self, rec: Sender<ZOSVirtualMemory>);
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZOSIPNet {
    #[serde(rename = "IP")]
    #[serde_as(as = "Bytes")]
    pub ip: Vec<u8>,

    #[serde_as(as = "Bytes")]
    #[serde(rename = "Mask")]
    pub mask: Vec<u8>,
}
impl ZOSIPNet {
    pub fn to_string(&self) -> String {
        let mut ip_str = String::from("");
        if self.ip.len() == 4 {
            let ip4 = Ipv4Addr::new(self.ip[0], self.ip[1], self.ip[2], self.ip[3]);
            ip_str = format!("{} {}", &ip_str, ip4.to_string());
        } else if self.ip.len() == 16 {
            let ip_arr: [u8; 16] = [
                self.ip[0],
                self.ip[1],
                self.ip[2],
                self.ip[3],
                self.ip[4],
                self.ip[5],
                self.ip[6],
                self.ip[7],
                self.ip[8],
                self.ip[9],
                self.ip[10],
                self.ip[11],
                self.ip[12],
                self.ip[13],
                self.ip[14],
                self.ip[15],
            ];

            let ip6 = Ipv6Addr::from(ip_arr);
            ip_str = format!("{} {}", &ip_str, ip6.to_string())
        }
        return ip_str.trim().to_string();
    }
}
impl From<IpNet> for ZOSIPNet {
    fn from(ipnet: IpNet) -> Self {
        ZOSIPNet {
            ip: ipnet.addr().to_string().as_bytes().to_vec(),
            mask: ipnet.netmask().to_string().as_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionPublicConfig {
    #[serde(rename = "IPv4")]
    pub ipv4: ZOSIPNet,
    #[serde(rename = "IPv6")]
    pub ipv6: ZOSIPNet,
    #[serde(rename = "HasPublicConfig")]
    pub has_public_config: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitDevice {
    // IsSingle is set to true if br-pub
    // is connected to zos bridge
    #[serde(rename = "IsSingle")]
    pub is_single: bool,
    // IsDual is set to true if br-pub is
    // connected to a physical nic
    #[serde(rename = "IsDual")]
    pub is_dual: bool,
    // AsDualInterface is set to the physical
    // interface name if IsDual is true
    #[serde(rename = "AsDualInterface")]
    pub as_dual_interface: String,
}
impl ExitDevice {
    pub fn to_string(&self) -> String {
        if self.is_single {
            String::from("Single")
        } else if self.is_dual {
            format!("Dual {}", self.as_dual_interface)
        } else {
            String::from("Unknown")
        }
    }
}

pub type NetlinkAddresses = Vec<ZOSIPNet>;
#[object(name = "network", version = "0.0.1")]
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
