//! Shared helpers for querying null-terminated strings from the native library.

use std::os::raw::c_char;

use boink_sys as sys;

/// Calls a native string query function that follows the (ptr,len) pattern.
///
/// The native function is expected to return `BOINK_ERR_BUFFER_TOO_SMALL` with
/// the required length on the first call and `BOINK_OK` once a buffer is provided.
///
/// A `BOINK_OK` response with a zero-length buffer is treated as "no value"
/// and results in `Ok(None)`.
pub(crate) fn query_string(
    func: unsafe extern "C" fn(*mut c_char, *mut u32) -> i32,
) -> Result<Option<String>, i32> {
    let mut len: u32 = 0;
    let mut code = unsafe { func(std::ptr::null_mut(), &mut len) };

    if code == sys::BOINK_ERR_INVALID_ARG {
        return Err(code);
    }

    if code == sys::BOINK_ERR_BUFFER_TOO_SMALL && len > 0 {
        let mut buf = vec![0u8; len as usize];
        code = unsafe { func(buf.as_mut_ptr() as *mut c_char, &mut len) };
        if code == sys::BOINK_OK {
            let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            let text = String::from_utf8_lossy(&buf[..nul]).to_string();
            return Ok(Some(text));
        }
        return Err(code);
    }

    if code == sys::BOINK_OK {
        return Ok(None);
    }

    Err(code)
}
