use std::path::Path;

const FLAGS_DIR: &str = "/tmp/flags";
pub enum Flags {
    LimitedCache,
}

impl AsRef<str> for Flags {
    fn as_ref(&self) -> &str {
        match self {
            Flags::LimitedCache => "limited-cache",
        }
    }
}

// CheckFlag checks the status of a flag based on a key
pub fn check(flag: Flags) -> bool {
    Path::new(FLAGS_DIR).join(flag.as_ref()).exists()
}
