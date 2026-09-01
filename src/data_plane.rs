//! Hub-owned wake-driven terminal data plane.

pub(crate) mod close_work;
pub(crate) mod driver;

pub(crate) use close_work::CloseWorkSource;
pub(crate) use driver::DataPlaneDriver;
