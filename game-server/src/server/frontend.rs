//! Standalone frontend HTTP server (static hosting + runtime config bootstrap).

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::header::{self, HeaderValue};
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::Config;

#[derive(Clone)]
struct FrontendRuntimeState {
    grpc_port: u16,
}

/// Runs the standalone frontend HTTP server until shutdown is requested.
pub async fn serve_frontend(
    cfg: Arc<Config>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !cfg.frontend_enable {
        tracing::info!("standalone frontend HTTP server disabled");
        return Ok(());
    }

    let index_file = cfg.frontend_dir.join("index.html");
    if !index_file.is_file() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "frontend index.html not found in FRONTEND_DIR: {}",
                cfg.frontend_dir.display()
            ),
        )));
    }

    let state = FrontendRuntimeState {
        grpc_port: cfg.listen_addr.port(),
    };
    let static_service =
        ServeDir::new(cfg.frontend_dir.clone()).not_found_service(ServeFile::new(index_file));

    let app = Router::new()
        .route("/config.js", get(config_js_handler))
        .fallback_service(static_service)
        .with_state(state);

    let listener = TcpListener::bind(cfg.frontend_listen_addr).await?;
    tracing::info!(
        listen_addr = %cfg.frontend_listen_addr,
        grpc_addr = %cfg.listen_addr,
        frontend_dir = %cfg.frontend_dir.display(),
        "standalone frontend HTTP server starting"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.recv().await;
            tracing::info!("standalone frontend HTTP shutdown requested");
        })
        .await
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

async fn config_js_handler(State(state): State<FrontendRuntimeState>) -> impl IntoResponse {
    let body = build_runtime_config_js(state.grpc_port);
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/javascript; charset=utf-8"),
        )],
        body,
    )
}

fn build_runtime_config_js(grpc_port: u16) -> String {
    // Bootstrap runtime env and default standalone endpoint settings in browser storage.
    format!(
        r#"(function () {{
  var origin = window.location.origin;
  var grpc = new URL(origin);
  grpc.port = "{grpc_port}";
  var backendUrl = grpc.origin;

  window.__ENV__ = Object.assign({{}}, window.__ENV__, {{
    APP_MODE: "standalone",
    IS_AUTH_ENABLED: "false",
    HA3_API_BASE_DEV: origin,
    HA3_API_BASE_PREPROD: origin,
    HA3_API_BASE_PROD: origin
  }});

  var seeded = {{
    grpcApiEnvironment: "custom",
    grpcCustomOrOverrideBackendUrl: backendUrl,
    grpcCustomOrOverrideHpsUrl: backendUrl,
    grpcCustomOrOverrideGametokenUrl: backendUrl,
    grpcCustomOrOverrideBrokerUrl: backendUrl,
    grpcCustomOrOverrideAchievementsUrl: backendUrl,
    ha3-manual-backend-url: backendUrl
  }};
  try {{
    for (var key in seeded) {{
      if (!Object.prototype.hasOwnProperty.call(seeded, key)) continue;
      if (window.localStorage.getItem(key) === null) {{
        window.localStorage.setItem(key, seeded[key]);
      }}
    }}
  }} catch (_err) {{
    // localStorage unavailable: continue without seeded defaults.
  }}
}})();
"#,
        grpc_port = grpc_port,
    )
}
