//! Helpers for querying optional engine metadata.

use std::os::raw::c_char;

use boink_sys as sys;

use crate::error::{Error, Result};

#[cfg(feature = "legacy-native-lib")]
use super::api::NativeApi;

/// Queries the native library for the engine build profile.
pub fn query_engine_profile() -> Result<Option<String>> {
    #[cfg(feature = "legacy-native-lib")]
    {
        let api = match NativeApi::instance() {
            Ok(api) => api,
            Err(_) => return Ok(None),
        };
        let func = match api.boink_get_engine_profile() {
            Some(func) => func,
            None => return Ok(None),
        };
        return query_string(|| func);
    }

    #[cfg(not(feature = "legacy-native-lib"))]
    {
        query_string(|| sys::boink_get_engine_profile)
    }
}

/// Queries the native library for the last error string, if available.
pub fn query_last_error() -> Result<Option<String>> {
    #[cfg(feature = "legacy-native-lib")]
    {
        let api = match NativeApi::instance() {
            Ok(api) => api,
            Err(_) => return Ok(None),
        };
        let func = match api.boink_get_last_error() {
            Some(func) => func,
            None => return Ok(None),
        };
        return query_string(|| func);
    }

    #[cfg(not(feature = "legacy-native-lib"))]
    {
        query_string(|| sys::boink_get_last_error)
    }
}

fn query_string<F>(resolve: F) -> Result<Option<String>>
where
    F: FnOnce() -> unsafe extern "C" fn(*mut c_char, *mut u32) -> i32,
{
    let func = resolve();
    let mut len: u32 = 0;
    let mut code = unsafe { func(std::ptr::null_mut(), &mut len) };

    if code == sys::BOINK_ERR_INVALID_ARG {
        return Err(Error::from_code(code));
    }

    if code == sys::BOINK_ERR_BUFFER_TOO_SMALL && len > 0 {
        let mut buf = vec![0u8; len as usize];
        code = unsafe { func(buf.as_mut_ptr() as *mut c_char, &mut len) };
        if code == sys::BOINK_OK {
            let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            let text = String::from_utf8_lossy(&buf[..nul]).to_string();
            return Ok(Some(text));
        }
        return Err(Error::from_code(code));
    }

    if code == sys::BOINK_OK {
        return Ok(Some(String::new()));
    }

    Err(Error::from_code(code))
}
