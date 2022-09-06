# FList daemon
The flist daemon is responsible to maintain all flist mounts. the API for `flist` provide the following functionality:
- Mount an flist given flist url, unique mount name, and mount options
- Unmount an flist given name.
- Find flist hash given a mount name.
- Check if a mount is active
- others?

## Operation
FLIST root (working directory) is `$ROOT`

### Mounting
0-fs mount is read-only by design. The sole idea of an flist (0-fs) mount is to be able to access and read files from an flist. FListd on the other-hand need to provide both read-only and read-write mounts for an flist. It does this as follows:
- On a mount request. The daemon must first need to mount the flist as read-only under a known location:
  - The mount target is always `$ROOT/ro/<flist-hash>`
- there is only a single RO mount per flist. Another request to mount the same flist reuses the same RO mount
- On ReadOnly mount:
  - A bind mount is created from `$ROOT/ro/<flist-hash>` to `$ROOT/mountpoint/<name>`
- On ReadWrite mount:
  - A call to `storaged` is made to allocate a subvolume with requested space
  - An overlay mount is created on `$ROOT/mountpoint/<name>` using the lower-layer = `$ROOT/ro/<flist-hash>` and upper-layer `<storage-subvol-path>`

### Unmounting
- Always the mount target under `$ROOT/mountpoint/<name>` is always unmounted
- If ReadWrite mode, the storage-subvolume is deleted
- If no other mounts are using the same flist RO mount, it's also unmmounted
