use anyhow::{Context, Result};
use std::path::Path;
use std::path::PathBuf;
use tokio::{fs::OpenOptions, io::AsyncBufRead, io::AsyncBufReadExt, io::BufReader};

const MOUNT_INFO: &str = "/proc/mounts";

pub struct Mount {
    pub source: String,
    pub target: PathBuf,
    pub filesystem: String,
    pub options: String,
    pub dump: u8,
    pub pass: u8,
}

impl Mount {
    /// read one of mount options. Returns Some(Value) if flag is set.
    /// if flag has a value set (say subvol=abc) the Value is of Some(&str), otherwise None
    ///
    /// if options = "ro,subvol=/abc"
    ///
    /// matches!(mount.option("rw"), None) == true
    /// matches!(mount.option("ro"), Some(None)) == true
    /// matches!(mount.option("subvol"), Some(Some(v)) if v == "/abc") == true
    pub fn option<K: AsRef<str>>(&self, key: K) -> Option<Option<&str>> {
        let key = key.as_ref();
        self.options
            .split(',')
            .map(|p| p.splitn(2, '=').collect::<Vec<&str>>())
            .filter(|i| i[0] == key)
            .map(|i| if i.len() == 2 { Some(i[1]) } else { None })
            .next()
    }
}

/// mountpoint returns mount information of target if mount exists
pub async fn mountpoint<P: AsRef<Path>>(target: P) -> Result<Option<Mount>> {
    let mounts = mounts().await?;
    let target = target.as_ref();
    Ok(mounts.into_iter().find(|m| m.target == target))
}

/// mount info returns mount information of source mount. if source (say a disk or disk parition) is mounted
/// multiple times Vec will have more than one element.
/// note that source is not a "path" because source can be other things
pub async fn mountinfo<P: AsRef<str>>(source: P) -> Result<Vec<Mount>> {
    let mounts = mounts().await?;
    let source = source.as_ref();
    Ok(mounts.into_iter().filter(|m| m.source == source).collect())
}

/// list all mounts on the system
pub async fn mounts() -> Result<Vec<Mount>> {
    let file = OpenOptions::new().read(true).open(MOUNT_INFO).await?;

    parser_reader(BufReader::new(file)).await
}

async fn parser_reader<R: AsyncBufRead + Unpin>(reader: R) -> Result<Vec<Mount>> {
    let mut lines = reader.lines();
    let mut mounts = vec![];
    while let Some(line) = lines.next_line().await? {
        // parse each line
        /* EXAMPLES
        proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
        sys /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
        dev /dev devtmpfs rw,nosuid,relatime,size=8087648k,nr_inodes=2021912,mode=755,inode64 0 0
        run /run tmpfs rw,nosuid,nodev,relatime,mode=755,inode64 0 0
        efivarfs /sys/firmware/efi/efivars efivarfs rw,nosuid,nodev,noexec,relatime 0 0
        devpts /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0
        /dev/sdb2 / btrfs rw,relatime,ssd,space_cache,subvolid=256,subvol=/root 0 0
        */
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 6 {
            log::error!("invalid mount info line '{}'", line);
            continue;
        }
        let mount = Mount {
            source: parts[0].into(),
            target: parts[1].into(),
            filesystem: parts[2].into(),
            options: parts[3].into(),
            dump: parts[4]
                .parse()
                .with_context(|| format!("invalid dump value from line {}", line))?,
            pass: parts[5]
                .parse()
                .with_context(|| format!("invalid pass value from line {}", line))?,
        };
        mounts.push(mount);
    }

    Ok(mounts)
}

#[cfg(test)]
mod test {
    use super::Mount;
    use std::path::PathBuf;
    use tokio::io::BufReader;

    const MOUNTS: &str = r#"
tmpfs / tmpfs rw,relatime,size=1572864k 0 0
proc /proc proc rw,relatime 0 0
sysfs /sys sysfs rw,relatime 0 0
devtmpfs /dev devtmpfs rw,relatime,size=82435064k,nr_inodes=20608766,mode=755 0 0
devpts /dev/pts devpts rw,relatime,mode=600,ptmxmode=000 0 0
cgroup_root /sys/fs/cgroup tmpfs rw,relatime 0 0
pids /sys/fs/cgroup/pids cgroup rw,relatime,pids 0 0
cpuset /sys/fs/cgroup/cpuset cgroup rw,relatime,cpuset 0 0
cpu /sys/fs/cgroup/cpu cgroup rw,relatime,cpu 0 0
cpuacct /sys/fs/cgroup/cpuacct cgroup rw,relatime,cpuacct 0 0
blkio /sys/fs/cgroup/blkio cgroup rw,relatime,blkio 0 0
memory /sys/fs/cgroup/memory cgroup rw,relatime,memory 0 0
devices /sys/fs/cgroup/devices cgroup rw,relatime,devices 0 0
freezer /sys/fs/cgroup/freezer cgroup rw,relatime,freezer 0 0
net_cls /sys/fs/cgroup/net_cls cgroup rw,relatime,net_cls 0 0
perf_event /sys/fs/cgroup/perf_event cgroup rw,relatime,perf_event 0 0
net_prio /sys/fs/cgroup/net_prio cgroup rw,relatime,net_prio 0 0
hugetlb /sys/fs/cgroup/hugetlb cgroup rw,relatime,hugetlb 0 0
tmpfs /var/run/netns tmpfs rw,relatime,size=1572864k 0 0
tmpfs /var/run/netns tmpfs rw,relatime,size=1572864k 0 0
tmpfs /var/run/netns tmpfs rw,relatime,size=1572864k 0 0
none /var/run/cache/storage tmpfs rw,relatime,size=1024k 0 0
/dev/sda /mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260 btrfs rw,relatime,ssd,space_cache,subvolid=5,subvol=/ 0 0
/dev/sdb /mnt/d242f9c2-384c-4575-a551-fab1aecf7970 btrfs rw,relatime,space_cache,subvolid=5,subvol=/ 0 0
/dev/sda /var/cache btrfs rw,relatime,ssd,space_cache,subvolid=256,subvol=/zos-cache 0 0
2618 /var/cache/modules/flistd/ro/bc8d1f6fc1d6c33137466d3a69b68a94 fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
2618 /var/cache/modules/flistd/mountpoint/traefik:bc8d1f6fc1d6c33137466d3a69b68a94 fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
nsfs /var/run/netns/ndmz nsfs rw 0 0
none /var/run/cache/networkd tmpfs rw,relatime,size=51200k 0 0
none /var/run/cache/vmd tmpfs rw,relatime,size=51200k 0 0
3050 /var/cache/modules/flistd/ro/b623b3b159fa02652bb21c695a157b4d fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
overlay /var/cache/modules/flistd/mountpoint/b623b3b159fa02652bb21c695a157b4d overlay rw,noatime,lowerdir=/var/cache/modules/flistd/ro/b623b3b159fa02652bb21c695a157b4d,upperdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/b623b3b159fa02652bb21c695a157b4d/rw,workdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/b623b3b159fa02652bb21c695a157b4d/wd 0 0
nsfs /var/run/netns/zdb-ns-2e1aa5662fab nsfs rw 0 0
3252 /var/cache/modules/flistd/ro/96e07bb92a56612f6a5e5939eb314ffa fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
3252 /var/cache/modules/flistd/mountpoint/cloud-container:96e07bb92a56612f6a5e5939eb314ffa fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
3306 /var/cache/modules/flistd/ro/e4ab73b4ac31f44c0d4bc3dfe5d6858c fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
overlay /var/cache/modules/flistd/mountpoint/288-5475-owncloud_samehabouelsaad overlay rw,noatime,lowerdir=/var/cache/modules/flistd/ro/e4ab73b4ac31f44c0d4bc3dfe5d6858c,upperdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/rootfs:288-5475-owncloud_samehabouelsaad/rw,workdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/rootfs:288-5475-owncloud_samehabouelsaad/wd 0 0
nsfs /var/run/netns/n-S4F3ncMPp9Agf nsfs rw 0 0
nsfs /var/run/netns/qfs-ns-f26a41445f21 nsfs rw 0 0
21755 /var/cache/modules/flistd/ro/91d63080f5f7b6514682a39432ef4349 fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
overlay /var/cache/modules/flistd/mountpoint/647-10988-qsfs overlay rw,noatime,lowerdir=/var/cache/modules/flistd/ro/91d63080f5f7b6514682a39432ef4349,upperdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/647-10988-qsfs/rw,workdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/647-10988-qsfs/wd 0 0
/dev/sda /var/cache/modules/qsfsd/mounts/647-10988-qsfs btrfs rw,relatime,ssd,space_cache,subvolid=256,subvol=/zos-cache 0 0
zdbfs /var/cache/modules/qsfsd/mounts/647-10988-qsfs fuse.zdbfs rw,nosuid,nodev,relatime,user_id=0,group_id=0,allow_other 0 0
22051 /var/cache/modules/flistd/ro/f94b5407f2e8635bd1b6b3dac7fef2d9 fuse.g8ufs ro,nosuid,nodev,relatime,user_id=0,group_id=0,default_permissions,allow_other 0 0
overlay /var/cache/modules/flistd/mountpoint/647-10988-vm overlay rw,noatime,lowerdir=/var/cache/modules/flistd/ro/f94b5407f2e8635bd1b6b3dac7fef2d9,upperdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/rootfs:647-10988-vm/rw,workdir=/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/rootfs:647-10988-vm/wd 0 0
    "#;

    #[test]
    fn mount_options() {
        let opt = Mount {
            options: "rw,relatime,ssd,space_cache,subvolid=256,subvol=/zos-cache".into(),
            filesystem: "btrfs".into(),
            source: "/dev/sda".into(),
            target: "/mnt/target".into(),
            dump: 0,
            pass: 0,
        };

        assert!(matches!(opt.option("ro"), None));
        assert!(matches!(opt.option("rw"), Some(None)));
        assert!(matches!(opt.option("subvolid"), Some(Some(v)) if v == "256"));
        assert!(matches!(opt.option("subvol"), Some(Some(v)) if v == "/zos-cache"));
    }

    #[tokio::test]
    async fn parser() {
        let mounts = super::parser_reader(BufReader::new(MOUNTS.as_bytes()))
            .await
            .expect("failed to parse mounts list");

        // find all overlay mounts
        let overlay: Vec<&Mount> = mounts.iter().filter(|m| m.source == "overlay").collect();
        assert_eq!(overlay.len(), 4);
        let mnt = overlay[0];
        assert_eq!(
            mnt.target,
            PathBuf::from("/var/cache/modules/flistd/mountpoint/b623b3b159fa02652bb21c695a157b4d"),
        );

        assert!(
            matches!(mnt.option("lowerdir"), Some(Some(v)) if v == "/var/cache/modules/flistd/ro/b623b3b159fa02652bb21c695a157b4d")
        );
        assert!(
            matches!(mnt.option("upperdir"), Some(Some(v)) if v == "/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/b623b3b159fa02652bb21c695a157b4d/rw")
        );
        assert!(
            matches!(mnt.option("workdir"), Some(Some(v)) if v == "/mnt/d7b5fb07-2b33-4ce6-87ad-5bf869211260/b623b3b159fa02652bb21c695a157b4d/wd")
        );
    }

    #[tokio::test]
    async fn parse_local() {
        let mnt = super::mountpoint("/")
            .await
            .expect("failed to read mountpoints");

        let mnt = mnt.expect("mount at / not found");

        assert_eq!(mnt.target, PathBuf::from("/"));
    }
}
