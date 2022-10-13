use std::path::PathBuf;

use anyhow::{Ok, Result};

use crate::Unit;
pub struct Usage {
    _size: Unit,
    _used: Unit,
}

// Volume struct is a btrfs subvolume
pub struct Volume {
    pub name: String,
    pub path: PathBuf,
    pub usage: Usage,
}
pub trait VolumeAllocator {
    // CreateFilesystem creates a filesystem with a given size. The filesystem
    // is mounted, and the path to the mountpoint is returned. The filesystem
    // is only attempted to be created in a pool of the given type. If no
    // more space is available in such a pool, `ErrNotEnoughSpace` is returned.
    // It is up to the caller to handle such a situation and decide if he wants
    // to try again on a different devicetype
    fn volume_create<S: AsRef<str>>(&self, name: S, size: Unit) -> Result<Volume>;

    // VolumeUpdate changes the size of an already existing volume
    fn volume_update<S: AsRef<str>>(&self, name: S, size: Unit) -> Result<()>;

    // ReleaseFilesystem signals that the named filesystem is no longer needed.
    // The filesystem will be unmounted and subsequently removed.
    // All data contained in the filesystem will be lost, and the
    // space which has been reserved for this filesystem will be reclaimed.
    fn volume_delete<S: AsRef<str>>(&self, name: S) -> Result<()>;
    // Path return the filesystem named name
    // if no filesystem with this name exists, an error is returned
    fn volume_lookup<S: AsRef<str>>(&self, name: S) -> Result<Volume>;
}
pub struct MockVolumeAllocator;

impl VolumeAllocator for MockVolumeAllocator {
    fn volume_create<S: AsRef<str>>(&self, name: S, _size: Unit) -> Result<Volume> {
        return Ok(Volume {
            name: name.as_ref().to_string(),
            path: PathBuf::from("/volumes/vol1"),
            usage: Usage { _size, _used: 0 },
        });
    }

    fn volume_update<S: AsRef<str>>(&self, _name: S, _size: Unit) -> Result<()> {
        Ok(())
    }

    fn volume_delete<S: AsRef<str>>(&self, _name: S) -> Result<()> {
        Ok(())
    }

    fn volume_lookup<S: AsRef<str>>(&self, name: S) -> Result<Volume> {
        return Ok(Volume {
            name: name.as_ref().to_string(),
            path: PathBuf::from("/volumes/vol1"),
            usage: Usage {
                _size: 100,
                _used: 0,
            },
        });
    }
}
