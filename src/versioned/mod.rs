use read_iter::ReadIter;
use semver::Version;
use std::fmt::Display;
use std::io::Read;
use std::str::{self, FromStr};
use std::{
    error::Error,
    fmt::{self, Debug, Formatter},
    fs,
    io::Write,
    os::unix::prelude::{OpenOptionsExt, PermissionsExt},
    result::Result::Ok,
};

#[derive(Debug, Clone)]
struct NotVersionedError {
    msg: String,
}

impl Display for NotVersionedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "no version information: {}", self.msg)
    }
}

impl Error for NotVersionedError {}

struct VersionedFile {
    pub version: Version,
    pub data_reader: Box<dyn Read>,
}

impl Read for VersionedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.data_reader.read(buf)
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.data_reader.read_exact(buf)
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        self.data_reader.read_to_end(buf)
    }
    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.data_reader.read_to_string(buf)
    }
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.data_reader.read_vectored(bufs)
    }
}

impl VersionedFile {
    fn new_writer<'a>(
        file: &'a mut std::fs::File,
        version: &Version,
    ) -> Result<Box<dyn Write + 'a>, Box<dyn Error>> {
        let v = serde_json::json!(version.to_string());
        match file.write_all(v.to_string().as_bytes()) {
            Ok(()) => (),
            Err(err) => return Err(Box::new(err)),
        };
        return Ok(Box::new(file));
    }

    fn write_file(
        path: &str,
        version: &Version,
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
        let mut writer = match VersionedFile::new_writer(&mut file, version) {
            Ok(writer) => writer,
            Err(err) => return Err(err),
        };
        match writer.write_all(data) {
            Ok(()) => (),
            Err(err) => return Err(Box::new(err)),
        }
        Ok(())
    }

    fn read_file(path: &str) -> Result<VersionedFile, Box<dyn Error>> {
        let file = match fs::OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(err) => return Err(Box::new(err)),
        };
        let new_reader = match VersionedFile::new_reader(Box::new(file)) {
            Ok(reader) => reader,
            Err(err) => return Err(err),
        };
        Ok(new_reader)
    }

    fn new_reader(r: Box<dyn std::io::Read>) -> Result<VersionedFile, Box<dyn Error>> {
        let mut iter = ReadIter::new(r);
        let mut reader = nop_json::Reader::new(&mut iter);
        let version_str: String = match reader.read() {
            Ok(res) => res,
            Err(err) => {
                return Err(Box::new(NotVersionedError {
                    msg: err.to_string(),
                }))
            }
        };
        match iter.last_error() {
            None => (),
            Some(err) => {
                return Err(Box::new(NotVersionedError {
                    msg: err.to_string(),
                }))
            }
        };
        let version = match Version::from_str(&version_str) {
            Ok(version) => version,
            Err(err) => {
                return Err(Box::new(NotVersionedError {
                    msg: err.to_string(),
                }))
            }
        };
        Ok(VersionedFile {
            version,
            data_reader: Box::new(iter),
        })
    }
}

#[cfg(test)]

mod test {
    use super::VersionedFile;
    use rand::Rng;
    use semver::Version;
    use std::fs;
    use std::io::Write;
    use std::str::FromStr;
    use std::{
        fs::Permissions,
        io::Read,
        os::unix::prelude::{OpenOptionsExt, PermissionsExt},
    };

    #[test]
    fn test_write_file() {
        let data = b"hellowrite";
        let version = Version::from_str("1.5.7-alpha").unwrap();
        let perm = Permissions::from_mode(0400);
        let res = VersionedFile::write_file("/tmp/test_write", &version, data, perm);
        assert!(res.is_ok());
    }

    #[test]
    fn test_read_file() {
        let mut file = match fs::OpenOptions::new()
            .mode(0400)
            .create(true)
            .write(true)
            .truncate(true)
            .open("/tmp/test_write")
        {
            Ok(file) => file,
            Err(err) => panic!("{}", err.to_string()),
        };
        match file.write_all("\"1.5.7-alpha\"helloworld".as_bytes()) {
            Ok(()) => (),
            Err(err) => panic!("{}", err.to_string()),
        };
        let version = Version::from_str("1.5.7-alpha").unwrap();
        let data: Vec<u8> = Vec::from("helloworld");
        let mut read_file = match VersionedFile::read_file("/tmp/test_write") {
            Ok(file) => file,
            Err(err) => panic!("{}", err.to_string()),
        };
        assert_eq!(version, read_file.version);

        let mut read_data: [u8; 10] = [0; 10];
        let res = read_file.data_reader.read_exact(&mut read_data);
        assert!(res.is_ok());
        assert_eq!(data, read_data);
    }

    #[test]
    fn test_write_read_file() {
        let data: Vec<u8> = (0..100)
            .map(|_| rand::thread_rng().gen_range(0..255))
            .collect();
        let version = Version::from_str("1.2.1-beta").unwrap();
        let res = VersionedFile::write_file(
            "/tmp/test_write",
            &version,
            &data,
            Permissions::from_mode(0400),
        );
        assert!(res.is_ok());
        let mut read_file = match VersionedFile::read_file("/tmp/test_write") {
            Ok(file) => file,
            Err(err) => panic!("{}", err.to_string()),
        };
        assert_eq!(version, read_file.version);
        let mut read_data: [u8; 100] = [0; 100];
        let res = read_file.data_reader.read_exact(&mut read_data);
        assert!(res.is_ok());
        assert_eq!(data, read_data);
    }
}
