use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRVersion {
    #[serde(rename = "VersionStr")]
    pub version_str: String,
    #[serde(rename = "VersionNum")]
    pub version_num: u64,
    #[serde(rename = "IsNum")]
    pub is_num: bool,
}

impl Display for PRVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_num {
            write!(f, "{}", self.version_num)
        } else {
            write!(f, "{}", self.version_str)
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

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;

        if let Some(pre) = &self.pre {
            write!(f, "_{}", pre[0])?;
            for pre_version in pre[1..].iter() {
                write!(f, ".{}", pre_version)?
            }
        }

        if let Some(build) = &self.build {
            write!(f, "+{}", build[0])?;
            for build_item in build[1..].iter() {
                write!(f, ".{}", build_item)?;
            }
        }

        Ok(())
    }
}
