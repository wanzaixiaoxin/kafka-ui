//! 非 Windows 平台的占位实现。
//!
//! 当前项目仅面向 Windows（NSIS 打包）。此模块保证代码在非 Windows 平台上可编译，
//! 但不提供实际加密——密码以明文存储。若需支持 macOS/Linux，应替换为
//! Keychain / Secret Service 实现。

const PREFIX: &str = "PLAIN:";

pub fn is_encrypted(s: &str) -> bool {
    s.starts_with(PREFIX)
}

pub fn encrypt_string(plaintext: &str) -> Result<String, String> {
    Ok(format!("{PREFIX}{plaintext}"))
}

pub fn decrypt_string(ciphertext: &str) -> Result<String, String> {
    ciphertext
        .strip_prefix(PREFIX)
        .map(String::from)
        .ok_or_else(|| "Not a known encrypted string".into())
}
