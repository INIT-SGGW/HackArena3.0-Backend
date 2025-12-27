//! gRPC/HTTP server entry points and shared middleware.

mod cors;
mod grpc;
mod shutdown;

pub use grpc::serve_grpc;
