use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::{
    fmt::Display,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

/// IP is a Golang compatible IP type
/// According to the Go docs (and net pkg implementation) a 16 byte array does not mean
/// it's an Ipv6. A 16 bytes array can still hold Ipv4 address [IETF RFC 4291 section 2.5.5.1](https://tools.ietf.org/html/rfc4291#section-2.5.5.1)
///
/// In the matter of fact, all Ipv4 methods in Go net pkg will always create a 16 bytes
/// array to hold the Ipv4. Hence the code here need to interpret the format of the IP
/// not the array length.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IP(ByteBuf);

impl From<IP> for IpAddr {
    fn from(ip: IP) -> Self {
        let inner = ip.0;
        if inner.len() == 4 {
            return IpAddr::V4(Ipv4Addr::new(inner[0], inner[1], inner[2], inner[3]));
        }
        // there must be a better way to do this
        let mut bytes: [u8; 16] = [0; 16];
        for (i, v) in inner.into_iter().take(16).enumerate() {
            bytes[i] = v;
        }
        let ipv6 = Ipv6Addr::from(bytes);
        if let Some(ipv4) = ipv6.to_ipv4() {
            IpAddr::V4(ipv4)
        } else {
            IpAddr::V6(ipv6)
        }
    }
}

impl From<&IP> for IpAddr {
    fn from(ip: &IP) -> Self {
        let inner = &ip.0;
        if inner.len() == 4 {
            return IpAddr::V4(Ipv4Addr::new(inner[0], inner[1], inner[2], inner[3]));
        }
        let mut bytes: [u8; 16] = [0; 16];
        for (i, v) in inner.iter().take(16).enumerate() {
            bytes[i] = *v;
        }
        let ipv6 = Ipv6Addr::from(bytes);
        if let Some(ipv4) = ipv6.to_ipv4() {
            IpAddr::V4(ipv4)
        } else {
            IpAddr::V6(ipv6)
        }
    }
}

impl Display for IP {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let addr: IpAddr = self.into();
        write!(f, "{}", addr)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IPMask(ByteBuf);

impl IPMask {
    pub fn bits(&self) -> u8 {
        let mut size: u8 = 0;
        for v in self.0.iter() {
            let mut x = *v;
            while x > 0 {
                x = x << 1;
                size += 1;
            }
        }
        size
    }
}

impl From<u8> for IPMask {
    fn from(size: u8) -> Self {
        // this is probably not the best way
        // to implement
        if size == 0 {
            return Self::default();
        }
        let mut v: Vec<u8> = vec![0];
        let mut index: usize = 0;
        for i in 0..size {
            v[index] = v[index] >> 1 | 0x80; // this is basically 0b1000 0000
            if v[index] == 0xff && i < size - 1 {
                // we only push new value if there is still more iterations
                v.push(0);
                index += 1;
            }
        }

        Self(ByteBuf::from(v))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IPNet {
    #[serde(rename = "IP")]
    pub ip: IP,

    #[serde(rename = "Mask")]
    pub mask: IPMask,
}

impl Display for IPNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.ip, self.mask.bits())
    }
}

/// you should never use this struct except to decode
/// IPNet structure that can be empty in Go. Because there
/// is no Option type in Golang, an empty struct in go has
/// all his attributes "zeroed" hence IP and Mask part of an
/// empty IPNet is nil. but not the struct itself of course.
/// hopefully we can avoid this type and similar types in the
/// future after either completely moving away from Go or
/// change the go types to use pointers that can be nil
/// (which is very unsafe refactor)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoIPNet {
    #[serde(rename = "IP")]
    ip: Option<IP>,

    #[serde(rename = "Mask")]
    mask: Option<IPMask>,
}

impl From<GoIPNet> for Option<IPNet> {
    fn from(o: GoIPNet) -> Self {
        match o.ip {
            Some(ip) => match o.mask {
                Some(mask) => Some(IPNet { ip, mask }),
                None => None,
            },
            None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum InterfaceType {
    #[serde(rename = "vlan")]
    VLan,
    #[serde(rename = "macvlan")]
    MacVLan,
    // because in go this can be empty string
    #[serde(rename = "")]
    Unknown,
}

impl Display for InterfaceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::VLan => write!(f, "vlan"),
            Self::MacVLan => write!(f, "macvlan"),
            Self::Unknown => write!(f, ""),
        }
    }
}

impl FromStr for InterfaceType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "vlan" => Ok(Self::VLan),
            "macvlan" => Ok(Self::MacVLan),
            "" => Ok(Self::Unknown),
            _ => Err("unknown interface type"),
        }
    }
}

// internal struct we use to be compatible with go types
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoPublicConfig {
    #[serde(rename = "Type")]
    typ: InterfaceType,
    #[serde(rename = "IPv4")]
    ipv4: GoIPNet,
    #[serde(rename = "IPv6")]
    ipv6: GoIPNet,
    #[serde(rename = "GW4")]
    gwv4: Option<IP>,
    #[serde(rename = "GW6")]
    gwv6: Option<IP>,
    #[serde(rename = "Domain")]
    domain: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicConfig {
    pub interface_type: InterfaceType,
    pub ipv4: Option<IPNet>,
    pub ipv6: Option<IPNet>,
    pub gwv4: Option<IP>,
    pub gwv6: Option<IP>,
    pub domain: Option<String>,
}

impl<'de> Deserialize<'de> for PublicConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let config = GoPublicConfig::deserialize(deserializer)?;
        Ok(Self {
            interface_type: config.typ,
            ipv4: config.ipv4.into(),
            ipv6: config.ipv6.into(),
            gwv4: config.gwv4,
            gwv6: config.gwv6,
            domain: config.domain,
        })
    }
}

/// compatibility struct with go because
/// we don't have Option in Go we had to
/// use flags.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionPublicConfig {
    #[serde(flatten)]
    pub config: PublicConfig,
    #[serde(rename = "HasPublicConfig")]
    pub is_set: bool,
}

impl From<OptionPublicConfig> for Option<PublicConfig> {
    fn from(o: OptionPublicConfig) -> Self {
        if o.is_set {
            Some(o.config)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoExitDevice {
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

#[derive(Debug, Clone, Serialize)]
pub enum ExitDevice {
    #[serde(rename = "vlan")]
    Single,
    #[serde(rename = "vlan")]
    Dual(String),
    #[serde(rename = "")]
    Unknown,
}
impl<'de> Deserialize<'de> for ExitDevice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let exit = GoExitDevice::deserialize(deserializer)?;
        if exit.is_single {
            Ok(Self::Single)
        } else if exit.is_dual {
            Ok(Self::Dual(exit.as_dual_interface))
        } else {
            Err(serde::de::Error::custom("unknown exit interface"))
        }
    }
}

impl Display for ExitDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Single => write!(f, "Single"),
            Self::Dual(_) => write!(f, "Dual"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod test {
    use serde::de::DeserializeOwned;

    use super::{ExitDevice, IPMask, IPNet, InterfaceType, OptionPublicConfig, PublicConfig, IP};

    use std::net::IpAddr;

    #[test]
    fn test_mask_bits() {
        let mask: IPMask = 16.into();
        assert!(mask.0[0] == 0xff);
        assert!(mask.0[1] == 0xff);

        assert!(mask.bits() == 16);

        let mask: IPMask = 18.into();
        assert!(mask.bits() == 18);
        assert!(mask.0[0] == 0xff);
        assert!(mask.0[1] == 0xff);
        assert!(mask.0[2] == 0b11000000);

        let mask: IPMask = 4.into();
        assert!(mask.bits() == 4);
        assert!(mask.0[0] == 0b11110000);

        let mask: IPMask = 6.into();
        assert!(mask.bits() == 6);
        assert!(mask.0[0] == 0b11111100);

        let mask: IPMask = 128.into();
        assert!(mask.bits() == 128);
        assert!(mask.0.len() == 16);
        assert!(mask.0.iter().all(|v| *v == 0xff));
    }

    fn decode<I: AsRef<str>, T: DeserializeOwned>(input: I) -> Result<T, rmp_serde::decode::Error> {
        let data = hex::decode(input.as_ref()).unwrap();
        // hexdump::hexdump(&data);
        rmp_serde::from_slice(&data)
    }

    #[test]
    fn test_go_compatibility() {
        // 192.168.1.20 (in a 16 bytes array)
        let data = "c41000000000000000000000ffffc0a80114";
        let ip: IP = decode(data).unwrap();
        let ip: IpAddr = ip.into();
        assert!(ip.to_string() == "192.168.1.20");

        // 2a10:b600:0:be77:f1d6:fc0:40ad:8b29
        let data = "c4102a10b6000000be77f1d60fc040ad8b29";
        let ip: IP = decode(data).unwrap();
        let ip: IpAddr = ip.into();
        assert!(ip.to_string() == "2a10:b600:0:be77:f1d6:fc0:40ad:8b29");

        // 192.168.1.0/24 (in ip net the ipv4 is actually in a 4 bytes array)
        let data = "82a24950c404c0a80100a44d61736bc404ffffff00";
        let net: IPNet = decode(data).unwrap();
        assert!(net.to_string() == "192.168.1.0/24");

        // 2a10:b600:0:be77::/64
        let data = "82a24950c4102a10b6000000be770000000000000000a44d61736bc410ffffffffffffffff0000000000000000";
        let net: IPNet = decode(data).unwrap();
        assert!(net.to_string() == "2a10:b600:0:be77::/64");

        // 2a10:b600:0:be77:f1d6:fc0:40ad:8b29/64
        let data = "82a24950c4102a10b6000000be77f1d60fc040ad8b29a44d61736bc410ffffffffffffffff0000000000000000";
        let net: IPNet = decode(data).unwrap();
        assert!(net.to_string() == "2a10:b600:0:be77:f1d6:fc0:40ad:8b29/64");
    }

    #[test]
    fn test_public_config() {
        //config {vlan 192.168.1.20/32 <nil> 192.168.1.1 <nil> }
        let data = "86a454797065a4766c616ea44950763482a24950c41000000000000000000000ffffc0a80114a44d61736bc404ffffffffa44950763682a24950c0a44d61736bc0a3475734c41000000000000000000000ffffc0a80101a3475736c0a6446f6d61696ea0";
        let config: PublicConfig = decode(data).unwrap();
        assert!(config.interface_type == InterfaceType::VLan);
        assert!(matches!(config.ipv4, Some(ip) if ip.to_string() == "192.168.1.20/32"));
        assert!(matches!(config.ipv6, None));
        assert!(matches!(&config.gwv4, Some(ip) if ip.to_string() == "192.168.1.1"));
        assert!(matches!(&config.gwv6, None));

        //option config {{vlan 192.168.1.20/32 <nil> 192.168.1.1 <nil> } true}
        let data = "87a454797065a4766c616ea44950763482a24950c41000000000000000000000ffffc0a80114a44d61736bc404ffffffffa44950763682a24950c0a44d61736bc0a3475734c41000000000000000000000ffffc0a80101a3475736c0a6446f6d61696ea0af4861735075626c6963436f6e666967c3";
        let config: OptionPublicConfig = decode(data).unwrap();
        let config: Option<PublicConfig> = config.into();
        assert!(config.is_some());
        let config = config.unwrap();
        assert!(config.interface_type == InterfaceType::VLan);
        assert!(matches!(config.ipv4, Some(ip) if ip.to_string() == "192.168.1.20/32"));
        assert!(matches!(config.ipv6, None));
        assert!(matches!(&config.gwv4, Some(ip) if ip.to_string() == "192.168.1.1"));
        assert!(matches!(&config.gwv6, None));

        // no config {{ <nil> <nil> <nil> <nil> } false}
        let data = "87a454797065a0a44950763482a24950c0a44d61736bc0a44950763682a24950c0a44d61736bc0a3475734c0a3475736c0a6446f6d61696ea0af4861735075626c6963436f6e666967c2";
        let config: OptionPublicConfig = decode(data).unwrap();
        let config: Option<PublicConfig> = config.into();
        assert!(config.is_none());
    }

    #[test]
    fn test_exit_device() {
        // single {true false }
        let data = "83a8497353696e676c65c3a649734475616cc2af41734475616c496e74657266616365a0";

        let exit: ExitDevice = decode(data).unwrap();
        assert!(matches!(exit, ExitDevice::Single));

        // dual (eth0) {false true eth0}
        let data =
            "83a8497353696e676c65c2a649734475616cc3af41734475616c496e74657266616365a465746830";
        let exit: ExitDevice = decode(data).unwrap();
        assert!(matches!(exit, ExitDevice::Dual(inf) if inf == "eth0"));

        // bad {false false }
        let data = "83a8497353696e676c65c2a649734475616cc2af41734475616c496e74657266616365a0";
        assert!(decode::<_, ExitDevice>(data).is_err());
    }
}
