use std::{fs, io::Error, fmt::{self, Display, Debug}, string, result::Result::Ok, io::Write};
use semver::{BuildMetadata, Prerelease, Version, VersionReq};
use std::str;
use tokio::io;


#[derive(Debug, Clone)]
struct NotVersionedError{
    msg: String
}

#[derive(Debug)]
struct VersionedFile{
    pub version: Version,
    pub data: Vec<u8>
}

impl NotVersionedError{
    fn new(msg: &str) -> NotVersionedError{
        return NotVersionedError{msg: msg.to_string()}
    }
}

impl fmt::Display for NotVersionedError{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "versioned file error: {}", self.msg)
    }
}



impl VersionedFile{
    
    fn read_file(path: &str) -> Result<VersionedFile, NotVersionedError> {
        
        let file = match fs::read(path){
            Ok(file) => file,
            Err(err) => return Err(NotVersionedError::new(err.to_string().as_str()))
        };
        let mut double_qoutes = 0;
        let mut version_end = 0;
        for (ind, c) in file.iter().enumerate(){
            if *c == b'\"'{
                double_qoutes += 1;
                if double_qoutes == 2{
                    version_end = ind;
                    break;
                }
            }
        }
        if double_qoutes < 2{
            return Err(NotVersionedError::new("no version information"))
        }
        let (version_bytes, data_bytes) = file.split_at(version_end+1);
        let version_str = match str::from_utf8(&version_bytes[1..version_end]){
            Ok(version) => version,
            Err(err) => return Err(NotVersionedError::new(err.to_string().as_str()))
        };

        let version = match Version::parse(version_str){
            Ok(version) => version,
            Err(err) => return Err(NotVersionedError::new(err.to_string().as_str()))
        };
        let ret : VersionedFile = VersionedFile { version, data: Vec::from(data_bytes) };
        Ok(ret)
    }

    fn write_file(path: &str, version: Version, data: &[u8]) -> Result<(), NotVersionedError>{
        let mut file = match fs::OpenOptions::new().write(true).create(true).open(path){
            Ok(file) => file,
            Err(err) => return Err(NotVersionedError::new(err.to_string().as_str()))
        };
        let f = [format!("\"{}\"", version).as_bytes(), data].concat();
        match file.write(&f[..]){
            Ok(res) => res,
            Err(err) => return Err(NotVersionedError::new(err.to_string().as_str()))
        };
        return Ok(())
    }
}

#[cfg(test)]

mod test{
    use std::io::Write;
    use semver::{Version, Prerelease};
    use super::VersionedFile;

    #[test]
    fn test_read_file(){
        let mut file = std::fs::OpenOptions::new().read(true).write(true).create(true).open("/tmp/test_read").unwrap();
        let data = b"hello read";
        let mut version = Version::new(1, 5, 7);
        version.pre = Prerelease::new("beta").unwrap();
        let f = [format!("\"{}\"", version).as_bytes(), data].concat();
        file.write(&f[..]).expect("couldn't write to file");
        let versioned = VersionedFile::read_file("/tmp/test_read");
        assert!(versioned.is_ok());
        let ret_file = versioned.unwrap();
        assert_eq!(ret_file.version, version);
        assert_eq!(ret_file.data, data);
    }

    #[test]
    fn test_write_file(){
        let data = b"hello write";
        let mut version = Version::new(1, 5, 7);
        version.pre = Prerelease::new("alpha").unwrap();
        let res = VersionedFile::write_file("/tmp/test_write", version, data);
        assert!(res.is_ok())
        
    }
}

