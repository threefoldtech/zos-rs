use std::path::PathBuf;

use anyhow::Result;

use crate::Unit;
pub struct Usage {
    size: Unit,
    used: Unit,
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
    fn create<S: AsRef<str>>(&self, name: S, size: Unit) -> Result<Volume>;

    // VolumeUpdate changes the size of an already existing volume
    fn update<S: AsRef<str>>(&self, name: S, size: Unit) -> Result<()>;

    // ReleaseFilesystem signals that the named filesystem is no longer needed.
    // The filesystem will be unmounted and subsequently removed.
    // All data contained in the filesystem will be lost, and the
    // space which has been reserved for this filesystem will be reclaimed.
    fn delete<S: AsRef<str>>(&self, cname: S) -> Result<()>;
    // Path return the filesystem named name
    // if no filesystem with this name exists, an error is returned
    fn lookup<S: AsRef<str>>(&self, name: S) -> Result<Volume>;
}