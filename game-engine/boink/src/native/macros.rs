//! Internal macros for string symbol queries.

/// Queries a native string symbol.
///
/// In legacy mode symbols are resolved from [`NativeApi`] and are required.
macro_rules! native_string_query {
    ($api_getter:ident, $func:path) => {{
        #[cfg(feature = "legacy-native-lib")]
        {
            let api = super::api::NativeApi::instance();
            let func = api.$api_getter();
            super::strings::query_string(func)
        }

        #[cfg(not(feature = "legacy-native-lib"))]
        {
            super::strings::query_string($func)
        }
    }};
}

pub(crate) use native_string_query;
