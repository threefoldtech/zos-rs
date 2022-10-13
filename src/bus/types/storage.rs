use std::path::PathBuf;
use crate::Unit;
use serde::{Deserialize, Serialize};

/// Defined the type of write volume
pub enum WriteLayer {
    /// If size, a new sub-volume is created. will
    /// be deleted once the mount is unmounted
    Size(Unit),
    /// Path to write layer
    Path(PathBuf),
}

/// MountMode
pub enum MountMode {
    ReadOnly,
    ReadWrite(WriteLayer),
}

/// MountOptions
pub struct MountOptions {
    /// Mode of mount
    pub mode: MountMode,
    /// Override default storage.
    pub storage: Option<String>,
}

impl MountOptions {
    /// creates a read-write mount options with a quote of (size)
    pub fn write(size: Unit) -> Self {
        MountOptions {
            mode: MountMode::ReadWrite(WriteLayer::Size(size)),
            storage: None,
        }
    }

    /// creates a read-write mount options with a write layer with given path
    pub fn path<S: Into<PathBuf>>(path: S) -> Self {
        MountOptions {
            mode: MountMode::ReadWrite(WriteLayer::Path(path.into())),
            storage: None,
        }
    }
}

impl Default for MountOptions {
    fn default() -> Self {
        MountOptions {
            mode: MountMode::ReadOnly,
            storage: None,
        }
    }
}

// TODO:
// Once go compatibility is not needed intermediate structure
// can be dropped. and use the default ser/de implementation with derive

// custom serialization to be compatible with the go implementation
// by building an intermediate structure
impl Serialize for MountOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let opts = GoMountOptions {
            read_only: matches!(self.mode, MountMode::ReadOnly),
            limit: if let MountMode::ReadWrite(WriteLayer::Size(size)) = self.mode {
                size
            } else {
                0
            },
            storage: if let Some(storage) = &self.storage {
                storage.clone()
            } else {
                String::default()
            },
            persisted_volume: if let MountMode::ReadWrite(WriteLayer::Path(path)) = &self.mode {
                path.to_path_buf()
            } else {
                PathBuf::from("")
            },
        };

        opts.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MountOptions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let opts: GoMountOptions = GoMountOptions::deserialize(deserializer)?;

        Ok(MountOptions {
            mode: if opts.read_only {
                MountMode::ReadOnly
            } else {
                if opts.persisted_volume.as_os_str().is_empty() {
                    MountMode::ReadWrite(WriteLayer::Size(opts.limit))
                } else {
                    MountMode::ReadWrite(WriteLayer::Path(opts.persisted_volume))
                }
            },
            storage: if !opts.storage.is_empty() {
                Some(opts.storage)
            } else {
                None
            },
        })
    }
}

#[derive(Serialize, Deserialize)]
struct GoMountOptions {
    #[serde(rename = "ReadOnly")]
    read_only: bool,
    #[serde(rename = "Limit")]
    limit: Unit,
    #[serde(rename = "Storage")]
    storage: String,
    #[serde(rename = "PersistedVolume")]
    persisted_volume: PathBuf,
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{MountMode, MountOptions, WriteLayer};
    use serde::de::DeserializeOwned;

    fn decode<I: AsRef<str>, T: DeserializeOwned>(input: I) -> Result<T, rmp_serde::decode::Error> {
        let data = hex::decode(input.as_ref()).unwrap();
        // hexdump::hexdump(&data);
        rmp_serde::from_slice(&data)
    }

    #[test]
    fn test_compatibility() {
        // read only: {ReadOnly:true Limit:0 Storage: PersistedVolume:}
        let data = "84a8526561644f6e6c79c3a54c696d6974cf0000000000000000a753746f72616765a0af506572736973746564566f6c756d65a0";
        let opts: MountOptions = decode(data).unwrap();
        assert!(matches!(opts.mode, MountMode::ReadOnly));
        assert_eq!(opts.storage, None);

        // read write with 250MB: {ReadOnly:false Limit:262144000 Storage: PersistedVolume:}
        let data = "84a8526561644f6e6c79c2a54c696d6974cf000000000fa00000a753746f72616765a0af506572736973746564566f6c756d65a0";
        let opts: MountOptions = decode(data).unwrap();
        assert!(matches!(
            opts.mode,
            MountMode::ReadWrite(WriteLayer::Size(size)) if size == 250 * crate::MEGABYTE
        ));
        assert_eq!(opts.storage, None);

        // read write with volume: {ReadOnly:false Limit:0 Storage: PersistedVolume:/some/path}
        let data = "84a8526561644f6e6c79c2a54c696d6974cf0000000000000000a753746f72616765a0af506572736973746564566f6c756d65aa2f736f6d652f70617468";
        let opts: MountOptions = decode(data).unwrap();
        assert!(matches!(
            opts.mode,
            MountMode::ReadWrite(WriteLayer::Path(path)) if path == PathBuf::from("/some/path")
        ));
        assert_eq!(opts.storage, None);

        // read write with volume and override storage: {ReadOnly:false Limit:0 Storage:https://custom.hub PersistedVolume:/some/path}
        let data = "84a8526561644f6e6c79c2a54c696d6974cf0000000000000000a753746f72616765b268747470733a2f2f637573746f6d2e687562af506572736973746564566f6c756d65aa2f736f6d652f70617468";
        let opts: MountOptions = decode(data).unwrap();
        assert!(matches!(
            opts.mode,
            MountMode::ReadWrite(WriteLayer::Path(path)) if path == PathBuf::from("/some/path")
        ));
        assert!(matches!(opts.storage, Some(storage) if storage == "https://custom.hub"));
    }
}
