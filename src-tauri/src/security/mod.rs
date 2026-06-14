//! 凭据加密模块。
//!
//! Windows: 使用 DPAPI (CryptProtectData / CryptUnprotectData) 加密 SASL 密码，
//! 密文以 Base64 形式内嵌在 data.json 中，仅当前 Windows 用户可解密。
//!
//! 非 Windows 平台: 暂不加密（项目当前仅面向 Windows）。

#[cfg(target_os = "windows")]
mod dpapi;

#[cfg(target_os = "windows")]
pub use dpapi::{decrypt_string, encrypt_string, is_encrypted};

#[cfg(not(target_os = "windows"))]
mod fallback;

#[cfg(not(target_os = "windows"))]
pub use fallback::{decrypt_string, encrypt_string, is_encrypted};
