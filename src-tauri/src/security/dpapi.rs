//! Windows DPAPI 加密实现。
//!
//! 使用 FFI 直接调用 `crypt32.dll` 的 `CryptProtectData` / `CryptUnprotectData`，
//! 无需引入额外的 `windows-sys` / `windows` 依赖。
//! DPAPI 使用当前用户的凭据派生密钥，因此密文只能在同一用户账户下解密。

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use std::ffi::c_void;

/// 加密字符串前缀，用于区分明文与密文（向后兼容已有明文数据）
const PREFIX: &str = "ENC1:";

/// CRYPT_INTEGER_BLOB — DPAPI 使用的二进制数据描述符
#[repr(C)]
struct DataBlob {
    cb_data: u32,
    pb_data: *mut u8,
}

#[link(name = "crypt32")]
extern "system" {
    fn CryptProtectData(
        data_in: *const DataBlob,
        data_desc: *const u16,
        entropy: *const DataBlob,
        reserved: *const c_void,
        prompt: *const c_void,
        flags: u32,
        data_out: *mut DataBlob,
    ) -> i32;

    fn CryptUnprotectData(
        data_in: *const DataBlob,
        data_desc: *mut *mut u16,
        entropy: *const DataBlob,
        reserved: *const c_void,
        prompt: *const c_void,
        flags: u32,
        data_out: *mut DataBlob,
    ) -> i32;
}

#[link(name = "kernel32")]
extern "system" {
    fn LocalFree(h_mem: *mut c_void) -> *mut c_void;
}

/// 判断字符串是否已被加密（以 `ENC1:` 前缀开头）
pub fn is_encrypted(s: &str) -> bool {
    s.starts_with(PREFIX)
}

/// 使用 DPAPI 加密字符串。
///
/// 返回 `ENC1:<base64>` 格式的密文。
/// 输入为空时原样返回空字符串。
pub fn encrypt_string(plaintext: &str) -> Result<String, String> {
    if plaintext.is_empty() {
        return Ok(String::new());
    }
    // is_encrypted 已经在上层调用前判断，这里做防御性检查
    if is_encrypted(plaintext) {
        return Ok(plaintext.to_string());
    }

    let bytes = plaintext.as_bytes();
    let in_blob = DataBlob {
        cb_data: bytes.len() as u32,
        pb_data: bytes.as_ptr() as *mut u8,
    };
    let mut out_blob = DataBlob {
        cb_data: 0,
        pb_data: std::ptr::null_mut(),
    };

    let ok = unsafe {
        CryptProtectData(
            &in_blob,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            &mut out_blob,
        )
    };

    if ok == 0 {
        return Err(format!(
            "CryptProtectData failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let encrypted = unsafe {
        let slice = std::slice::from_raw_parts(out_blob.pb_data, out_blob.cb_data as usize);
        slice.to_vec()
    };
    // DPAPI 要求调用方释放输出缓冲区
    unsafe {
        LocalFree(out_blob.pb_data as *mut c_void);
    }

    Ok(format!("{PREFIX}{}", BASE64.encode(&encrypted)))
}

/// 使用 DPAPI 解密字符串。
///
/// 输入须为 `ENC1:<base64>` 格式。
pub fn decrypt_string(ciphertext: &str) -> Result<String, String> {
    if ciphertext.is_empty() {
        return Ok(String::new());
    }

    let encoded = ciphertext
        .strip_prefix(PREFIX)
        .ok_or_else(|| "Not an encrypted string (missing ENC1: prefix)".to_string())?;

    let bytes = BASE64
        .decode(encoded)
        .map_err(|e| format!("Base64 decode failed: {e}"))?;

    let in_blob = DataBlob {
        cb_data: bytes.len() as u32,
        pb_data: bytes.as_ptr() as *mut u8,
    };
    let mut out_blob = DataBlob {
        cb_data: 0,
        pb_data: std::ptr::null_mut(),
    };

    let ok = unsafe {
        CryptUnprotectData(
            &in_blob,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            &mut out_blob,
        )
    };

    if ok == 0 {
        return Err(format!(
            "CryptUnprotectData failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let plaintext = unsafe {
        let slice = std::slice::from_raw_parts(out_blob.pb_data, out_blob.cb_data as usize);
        String::from_utf8_lossy(slice).into_owned()
    };
    unsafe {
        LocalFree(out_blob.pb_data as *mut c_void);
    }

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let plaintext = "my-secret-password-123!@#";
        let encrypted = encrypt_string(plaintext).unwrap();
        assert!(is_encrypted(&encrypted));
        assert_ne!(&encrypted, plaintext);
        let decrypted = decrypt_string(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn empty_string() {
        assert_eq!(encrypt_string("").unwrap(), "");
        assert_eq!(decrypt_string("").unwrap(), "");
    }

    #[test]
    fn plaintext_not_encrypted() {
        assert!(!is_encrypted("hello"));
        assert!(!is_encrypted(""));
    }
}
