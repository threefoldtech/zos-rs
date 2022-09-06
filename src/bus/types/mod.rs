/// types that are compatible with the Go implementations. Mainly to be used by the zbus to
/// allow communication between the Go and the Rust modules seamlessly
///
/// Types that has native rust implementations must have From and Into implementations from
/// those types.
pub mod net;
pub mod stats;
pub mod storage;
pub mod version;
