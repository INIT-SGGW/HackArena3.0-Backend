//! CORS configuration for gRPC-web requests.

#[cfg(all(feature = "local", feature = "standalone"))]
use tower_http::cors::AllowOrigin;
use tower_http::cors::{AllowHeaders, AllowMethods, CorsLayer};

use crate::config::Config;

/// Builds the CORS layer used by gRPC-web.
pub fn cors_layer(cfg: &Config) -> CorsLayer {
    #[cfg(all(feature = "local", feature = "standalone"))]
    let allow_origin = if cfg.cors_allow_any {
        // Frontend can still send `credentials: include`; wildcard origin would be rejected.
        AllowOrigin::mirror_request()
    } else {
        cfg.allow_origin.clone()
    };
    #[cfg(not(all(feature = "local", feature = "standalone")))]
    let allow_origin = cfg.allow_origin.clone();

    tracing::info!("CORS allow_origin: {:?}", allow_origin);

    let layer = CorsLayer::new()
        .allow_methods(AllowMethods::mirror_request())
        .allow_headers(AllowHeaders::mirror_request())
        .allow_origin(allow_origin)
        .expose_headers(cfg.expose_headers.clone());

    #[cfg(feature = "local")]
    let layer = layer.allow_credentials(true);

    layer
}
