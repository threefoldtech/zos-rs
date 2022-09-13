/// implementation of the flist daemon
use crate::bus::api::Flist;
use crate::bus::types::storage::MountOptions;
use anyhow::Result;
use std::path::Path;

pub struct FListDaemon;

#[async_trait::async_trait]
impl Flist for FListDaemon {
    async fn mount(name: String, url: String, options: MountOptions) -> Result<String> {
        unimplemented!()
    }

    async fn unmount(name: String) -> Result<()> {
        unimplemented!()
    }

    async fn update(name: String, size: crate::Unit) -> Result<String> {
        unimplemented!()
    }

    async fn exists(name: String) -> Result<bool> {
        unimplemented!()
    }

    async fn hash_of_flist(url: String) -> Result<String> {
        unimplemented!()
    }

    async fn hash_of_mount(name: String) -> Result<String> {
        unimplemented!()
    }
}
