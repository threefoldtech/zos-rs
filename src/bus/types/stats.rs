use crate::Unit;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VirtualMemory {
    #[serde(rename = "Total")]
    pub total: u64,
    #[serde(rename = "Available")]
    pub available: u64,
    #[serde(rename = "Used")]
    pub used: u64,
    #[serde(rename = "UsedPercent")]
    pub used_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimesStat {
    #[serde(rename = "Percent")]
    pub percent: f64,
}
