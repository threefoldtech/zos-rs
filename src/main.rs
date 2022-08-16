use ipnet::IpNet;
use rbus::{self, client::Receiver};
use std::net::Ipv6Addr;
use std::time::Duration;
use std::{env, net::Ipv4Addr};
use zos_traits::{NetlinkAddresses, ZOSIPNet};

use crate::{
    app::Stubs,
    zos_traits::{
        IdentityManagerStub, NetworkerStub, RegistrarStub, StatisticsStub, SystemMonitorStub,
        VersionMonitorStub,
    },
};
mod app;
mod zos_traits;
use core::result::Result;
use std::error::Error;
mod ui;
mod zui;
use crate::zos_traits::Capacity;
use crate::zos_traits::Version;
use crate::zui::run;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    const IDENTITY_MOD: &str = "identityd";
    let iden_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let identity_manager = IdentityManagerStub::new(IDENTITY_MOD, iden_cli);

    let ver_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let version_monitor = VersionMonitorStub::new(IDENTITY_MOD, ver_cli);

    const REGISTRAR_MOD: &str = "registrar";
    let reg_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let registrar = RegistrarStub::new(REGISTRAR_MOD, reg_cli);

    const PROVISION_MOD: &str = "provision";
    let statistics_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let statistics = StatisticsStub::new(PROVISION_MOD, statistics_cli);

    const NODE_MOD: &str = "node";
    let sys_monitor_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let sys_monitor = SystemMonitorStub::new(NODE_MOD, sys_monitor_cli);

    const NETWORK_MOD: &str = "network";
    let network_cli = rbus::Client::new("redis://0.0.0.0:6379").await.unwrap();
    let network = NetworkerStub::new(NETWORK_MOD, network_cli);

    let stubs = Stubs {
        identity_manager,
        registrar,
        version_monitor,
        statistics,
        sys_monitor,
        network,
    };
    // let ignore_case = env::var("IGNORE_CASE").unwrap();
    let tick_rate = Duration::from_millis(250);
    run(stubs, tick_rate, true).await?;

    // let mut recev: Receiver<NetlinkAddresses> = loop {
    //     match network.zos_addresses().await {
    //         Ok(recev) => {
    //             break recev;
    //         }
    //         Err(err) => {
    //             println!("Error executing version method: {}", err);
    //             continue;
    //         }
    //     };
    // };
    // loop {
    //     let cpu = match recev.recv().await {
    //         Some(res) => match res {
    //             Ok(cpu) => cpu,
    //             Err(err) => {
    //                 println!("Error getting ZOS IP usage: {}", err);
    //                 continue;
    //             }
    //         },
    //         None => {
    //             println!("None");
    //             continue;
    //         }
    //     };
    //     let mut ip_str = String::from("");
    //     for entry in cpu {
    //         ip_str = format!("{} {}", ip_str, entry.to_string());
    //     }
    //     println!("ZOS IP: {}", &ip_str)
    // }

    Ok(())
}
