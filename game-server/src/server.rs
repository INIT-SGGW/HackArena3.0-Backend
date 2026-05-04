//! gRPC/HTTP server entry points and shared middleware.

mod cors;
#[cfg(feature = "standalone")]
mod frontend;
mod grpc;
pub mod shutdown;

#[cfg(feature = "standalone")]
pub use frontend::serve_frontend;
pub use grpc::serve_grpc;
