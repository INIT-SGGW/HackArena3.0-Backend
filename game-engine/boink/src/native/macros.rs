//! Internal macros for optional legacy symbol handling.

/// Queries an optional native string symbol, falling back to `None` if missing.
///
/// In legacy mode this resolves symbols dynamically and logs a one-time warning
/// when a symbol is absent.
macro_rules! native_optional_string_query {
    ($api_getter:ident, $func:path) => {{
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = match super::api::NativeApi::instance() {
                Ok(api) => api,
                Err(_) => return Ok(None),
            };
            let func = match api.$api_getter() {
                Some(func) => func,
                None => {
                    static ONCE: std::sync::Once = std::sync::Once::new();
                    ONCE.call_once(|| {
                        tracing::warn!("Boink native symbol missing: {}", stringify!($api_getter));
                    });
                    return Ok(None);
                }
            };
            super::strings::query_string(func)
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            super::strings::query_string($func)
        }
    }};
}

pub(crate) use native_optional_string_query;
