use anyhow::Result;
use rbus::client::Receiver;

use crate::zos_traits::{
    Capacity, ExitDevice, IdentityManagerStub, NetlinkAddresses, NetworkerStub, OptionPublicConfig,
    RegistrarStub, StatisticsStub, SystemMonitorStub, Version, VersionMonitorStub, ZOSTimesStat,
    ZOSVirtualMemory,
};

pub struct Stubs {
    pub identity_manager: IdentityManagerStub,
    pub registrar: RegistrarStub,
    pub version_monitor: VersionMonitorStub,
    pub statistics: StatisticsStub,
    pub sys_monitor: SystemMonitorStub,
    pub network: NetworkerStub,
}
use std::sync::{Arc, Mutex};
pub struct App {
    pub stubs: Stubs,
    pub node_id: Result<u32, rbus::protocol::Error>,
    pub farm_id: Result<u32, rbus::protocol::Error>,
    pub exit_device: Result<ExitDevice, rbus::protocol::Error>,
    pub farm_name: Result<String, rbus::protocol::Error>,
    pub should_quit: bool,
    pub version: Arc<Mutex<String>>,
    pub used_mem_percent: Arc<Mutex<f64>>,
    pub used_cpu_percent: Arc<Mutex<f64>>,
    pub capacity: Arc<Mutex<Capacity>>,
    pub zos_addresses: Arc<Mutex<String>>,
    pub dmz_addresses: Arc<Mutex<String>>,
    pub ygg_addresses: Arc<Mutex<String>>,
    pub pub_addresses: Arc<Mutex<String>>,
}

impl App {
    pub fn new(stubs: Stubs, enhanced_graphics: bool) -> App {
        App {
            stubs,
            node_id: Ok(0),
            farm_id: Ok(0),
            farm_name: Ok(String::from("")),
            should_quit: false,
            version: Arc::new(Mutex::new(String::from("0.0.0"))),
            capacity: Arc::new(Mutex::new(Capacity {
                cru: 0,
                sru: 0,
                hru: 0,
                mru: 0,
                ipv4u: 0,
            })),
            used_mem_percent: Arc::new(Mutex::new(0.0)),
            used_cpu_percent: Arc::new(Mutex::new(0.0)),
            zos_addresses: Arc::new(Mutex::new(String::from("Not Configured"))),
            dmz_addresses: Arc::new(Mutex::new(String::from("Not Configured"))),
            ygg_addresses: Arc::new(Mutex::new(String::from("Not Configured"))),
            pub_addresses: Arc::new(Mutex::new(String::from("No public config"))),
            exit_device: Ok(ExitDevice {
                is_single: false,
                is_dual: false,
                as_dual_interface: String::from(""),
            }),
        }
    }

    pub fn on_key(&mut self, c: char) {
        match c {
            'q' => {
                self.should_quit = true;
            }
            _ => {}
        }
    }
    pub async fn poll_version(&self) {
        let mut recev: Receiver<Version> = loop {
            match self.stubs.version_monitor.version().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let version_state = Arc::clone(&self.version);
            async move {
                loop {
                    let version = match recev.recv().await {
                        Some(res) => match res {
                            Ok(version) => version,
                            Err(err) => {
                                log::error!("Error getting version: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    *version_state.lock().unwrap() = version.to_string();
                }
            }
        });
    }
    pub async fn poll_memory_usage(&self) {
        let mut recev: Receiver<ZOSVirtualMemory> = loop {
            match self.stubs.sys_monitor.memory().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let used_mem_percent = Arc::clone(&self.used_mem_percent);
            async move {
                loop {
                    let mem = match recev.recv().await {
                        Some(res) => match res {
                            Ok(mem) => mem,
                            Err(err) => {
                                log::error!("Error getting Memory usage: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    *used_mem_percent.lock().unwrap() = mem.used_percent;
                }
            }
        });
    }
    pub async fn poll_cpu_usage(&self) {
        let mut recev: Receiver<ZOSTimesStat> = loop {
            match self.stubs.sys_monitor.cpu().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let used_cpu_percent = Arc::clone(&self.used_cpu_percent);
            async move {
                loop {
                    let cpu = match recev.recv().await {
                        Some(res) => match res {
                            Ok(cpu) => cpu,
                            Err(err) => {
                                println!("Error getting CPU usage: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    *used_cpu_percent.lock().unwrap() = cpu.percent;
                }
            }
        });
    }

    pub async fn poll_reserved_stream(&self) {
        let mut recev: Receiver<Capacity> = loop {
            match self.stubs.statistics.reserved_stream().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error getting reserved capacity method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let capacity_state = Arc::clone(&self.capacity);
            async move {
                loop {
                    let capacity = match recev.recv().await {
                        Some(res) => match res {
                            Ok(version) => version,
                            Err(err) => {
                                log::error!("Error getting version: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    *capacity_state.lock().unwrap() = capacity;
                }
            }
        });
    }

    pub async fn poll_zos_addresses(&self) {
        let mut recev: Receiver<NetlinkAddresses> = loop {
            match self.stubs.network.zos_addresses().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let zos_addresses_state = Arc::clone(&self.zos_addresses);
            async move {
                loop {
                    let zos_addresses = match recev.recv().await {
                        Some(res) => match res {
                            Ok(zos_addresses) => zos_addresses,
                            Err(err) => {
                                log::error!("Error getting zos addresses: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    let mut zos_addresses_str = String::from("");
                    for address in zos_addresses.iter() {
                        zos_addresses_str =
                            format!("{} {}", &zos_addresses_str, address.to_string())
                    }
                    *zos_addresses_state.lock().unwrap() = zos_addresses_str.trim().to_string();
                }
            }
        });
    }
    pub async fn poll_dmz_addresses(&self) {
        let mut recev: Receiver<NetlinkAddresses> = loop {
            match self.stubs.network.dmz_addresses().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let dmz_addresses_state = Arc::clone(&self.dmz_addresses);
            async move {
                loop {
                    let dmz_addresses = match recev.recv().await {
                        Some(res) => match res {
                            Ok(dmz_addresses) => dmz_addresses,
                            Err(err) => {
                                log::error!("Error getting dmz addresses: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    let mut dmz_addresses_str = String::from("");
                    for address in dmz_addresses.iter() {
                        dmz_addresses_str =
                            format!("{} {}", &dmz_addresses_str, address.to_string())
                    }
                    *dmz_addresses_state.lock().unwrap() = dmz_addresses_str.trim().to_string();
                }
            }
        });
    }
    pub async fn poll_ygg_addresses(&self) {
        let mut recev: Receiver<NetlinkAddresses> = loop {
            match self.stubs.network.ygg_addresses().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let ygg_addresses_state = Arc::clone(&self.ygg_addresses);
            async move {
                loop {
                    let ygg_addresses = match recev.recv().await {
                        Some(res) => match res {
                            Ok(ygg_addresses) => ygg_addresses,
                            Err(err) => {
                                log::error!("Error getting ygg addresses: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    let mut ygg_addresses_str = String::from("");
                    for address in ygg_addresses.iter() {
                        ygg_addresses_str =
                            format!("{} {}", &ygg_addresses_str, address.to_string())
                    }
                    *ygg_addresses_state.lock().unwrap() = ygg_addresses_str.trim().to_string();
                }
            }
        });
    }
    pub async fn poll_public_addresses(&self) {
        let mut recev: Receiver<OptionPublicConfig> = loop {
            match self.stubs.network.public_addresses().await {
                Ok(recev) => {
                    break recev;
                }
                Err(err) => {
                    log::error!("Error executing version method: {}", err);
                    continue;
                }
            };
        };
        tokio::spawn({
            let pub_addresses_state = Arc::clone(&self.pub_addresses);
            async move {
                loop {
                    let pub_addresses = match recev.recv().await {
                        Some(res) => match res {
                            Ok(pub_addresses) => pub_addresses,
                            Err(err) => {
                                log::error!("Error getting ygg addresses: {}", err);
                                continue;
                            }
                        },
                        None => continue,
                    };
                    if !pub_addresses.has_public_config {
                        *pub_addresses_state.lock().unwrap() = String::from("No public config");
                    } else {
                        *pub_addresses_state.lock().unwrap() = format!(
                            "{} {}",
                            pub_addresses.ipv4.to_string(),
                            pub_addresses.ipv6.to_string()
                        );
                    }
                }
            }
        });
    }
    pub async fn on_tick(&mut self) {
        // Update progress
        self.node_id = self.stubs.registrar.node_id().await;
        self.farm_id = self.stubs.identity_manager.farm_id().await;
        self.farm_name = self.stubs.identity_manager.farm().await;
        self.exit_device = self.stubs.network.get_public_exit_device().await;
    }
}
