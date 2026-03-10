//! CORS configuration for gRPC-web requests.

use tower_http::cors::{AllowHeaders, AllowMethods, CorsLayer};

use crate::config::Config;

/// Builds the CORS layer used by gRPC-web.
pub fn cors_layer(cfg: &Config) -> CorsLayer {
    tracing::info!("CORS allow_origin: {:?}", cfg.allow_origin);

    let layer = CorsLayer::new()
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request())
        .allow_origin(cfg.allow_origin.clone())
        .expose_headers(cfg.expose_headers.clone());

    #[cfg(feature = "local")]
    let layer = layer.allow_credentials(true);

    layer
}
