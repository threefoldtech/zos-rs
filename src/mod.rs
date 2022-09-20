pub mod app;
pub mod bus;
pub mod cache;
pub mod env;
pub mod flist;
pub mod kernel;
pub mod storage;
pub mod system;

// Unit is a size measuring unit 1 unit = 1 byte
pub type Unit = u64;

pub const KILOBYTE: Unit = 1024;
pub const MEGABYTE: Unit = 1024 * KILOBYTE;
pub const GIGABYTE: Unit = 1024 * MEGABYTE;
pub const TERABYTE: Unit = 1024 * GIGABYTE;

#[cfg(test)]
mod test {
    #[test]
    fn test_unit() {
        assert_eq!(10 * super::KILOBYTE, 10 * 1024);
        assert_eq!(20 * super::MEGABYTE, 20 * 1024 * 1024);
        assert_eq!(30 * super::GIGABYTE, 30 * 1024 * 1024 * 1024);
        assert_eq!(40 * super::TERABYTE, 40 * 1024 * 1024 * 1024 * 1024);
    }
}
