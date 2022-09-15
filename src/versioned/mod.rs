use semver::Version;
use std::str;
use std::{
    error::Error,
    fmt::{self, Debug},
    fs,
    io::Write,
    os::unix::prelude::{OpenOptionsExt, PermissionsExt},
    result::Result::Ok,
};

#[derive(Debug, Clone)]
struct NotVersionedError;

#[derive(Debug)]
struct VersionedFile {
    pub version: Version,
    pub data: Vec<u8>,
}

impl fmt::Display for NotVersionedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no version information")
    }
}

impl Error for NotVersionedError {}

impl VersionedFile {
    fn new_writer(
        file: &mut std::fs::File,
        version: Version,
    ) -> Result<Box<dyn Write + '_>, Box<dyn Error>> {
        match file.write_all(format!("\"{}\"", version).as_bytes()) {
            Ok(()) => (),
            Err(err) => return Err(Box::new(err)),
        };
        return Ok(Box::new(file));
    }

    fn write_file(
        path: &str,
        version: Version,
        data: &[u8],
        perm: fs::Permissions,
    ) -> Result<(), Box<dyn Error>> {
        let mut file = match fs::OpenOptions::new()
            .mode(perm.mode())
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
        {
            Ok(file) => file,
            Err(err) => return Err(Box::new(err)),
        };
        let mut v_writer = match VersionedFile::new_writer(&mut file, version) {
            Ok(writer) => writer,
            Err(err) => return Err(err),
        };
        match v_writer.write_all(data) {
            Ok(()) => (),
            Err(err) => return Err(Box::new(err)),
        }
        Ok(())
    }
}

#[cfg(test)]

mod test {
    use super::VersionedFile;
    use semver::Version;
    use std::{fs::Permissions, os::unix::prelude::PermissionsExt};

    #[test]
    fn test_write_file() {
        let data = b"hello write";
        let version = Version::new(1, 5, 7);
        let perm = Permissions::from_mode(777);
        let res = VersionedFile::write_file("/tmp/test_write", version, data, perm);
        assert!(res.is_ok());
    }
}
