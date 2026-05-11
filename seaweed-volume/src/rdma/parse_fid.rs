//! Parse a SeaweedFS fid into `(volume_id, needle_id, cookie)`.
//!
//! SeaweedFS fids use:
//!
//! ```text
//! <volume_id>,<needle_id_hex><cookie_hex>[_<delta>]
//! ```
//!
//! The last 8 hex chars of the key half are the 32-bit cookie. The
//! preceding hex chars are the needle id. An optional decimal `_delta`
//! suffix is added to the needle id.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseFidError {
    #[error("fid is empty")]
    Empty,
    #[error("fid missing comma separator")]
    MissingComma,
    #[error("fid has empty volume_id half")]
    EmptyVolume,
    #[error("fid has empty key half")]
    EmptyKey,
    #[error("fid key+cookie hex too short (need >8 chars to leave room for cookie)")]
    KeyTooShort,
    #[error("invalid volume_id: {0}")]
    InvalidVolume(String),
    #[error("invalid key hex: {0}")]
    InvalidKey(String),
    #[error("invalid cookie hex: {0}")]
    InvalidCookie(String),
    #[error("invalid delta: {0}")]
    InvalidDelta(String),
}

pub fn parse_fid(s: &str) -> Result<(u32, u64, u32), ParseFidError> {
    if s.is_empty() {
        return Err(ParseFidError::Empty);
    }

    let comma = s.find(',').ok_or(ParseFidError::MissingComma)?;
    let (vid_part, rest) = (&s[..comma], &s[comma + 1..]);
    if vid_part.is_empty() {
        return Err(ParseFidError::EmptyVolume);
    }
    if rest.is_empty() {
        return Err(ParseFidError::EmptyKey);
    }

    let vid: u32 = vid_part
        .parse()
        .map_err(|_| ParseFidError::InvalidVolume(vid_part.to_string()))?;

    let (key_hex, delta) = match rest.find('_') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    if key_hex.is_empty() {
        return Err(ParseFidError::EmptyKey);
    }
    if key_hex.len() <= 8 {
        return Err(ParseFidError::KeyTooShort);
    }

    let split = key_hex.len() - 8;
    let needle_hex = &key_hex[..split];
    let cookie_hex = &key_hex[split..];

    let mut needle_id = u64::from_str_radix(needle_hex, 16)
        .map_err(|_| ParseFidError::InvalidKey(needle_hex.to_string()))?;
    let cookie = u32::from_str_radix(cookie_hex, 16)
        .map_err(|_| ParseFidError::InvalidCookie(cookie_hex.to_string()))?;

    if !delta.is_empty() {
        let d: u64 = delta
            .parse()
            .map_err(|_| ParseFidError::InvalidDelta(delta.to_string()))?;
        needle_id += d;
    }

    Ok((vid, needle_id, cookie))
}

#[cfg(test)]
mod tests {
    use super::{parse_fid, ParseFidError};

    #[test]
    fn parse_fid_extracts_volume_key_and_cookie() {
        assert_eq!(parse_fid("3,0189b26a98").unwrap(), (3, 0x01, 0x89b26a98));
        assert_eq!(parse_fid("3,a12345678").unwrap(), (3, 0x0a, 0x12345678));
        assert_eq!(
            parse_fid("3,deadbeef89abcdef").unwrap(),
            (3, 0xdeadbeef, 0x89abcdef)
        );
        assert_eq!(parse_fid("12345,abc1234567").unwrap().0, 12345);
        assert_eq!(parse_fid("3,000189b26a98").unwrap().1, 0x0001);
    }

    #[test]
    fn parse_fid_applies_delta_suffix() {
        assert_eq!(parse_fid("3,0189b26a98_5").unwrap(), (3, 0x06, 0x89b26a98));
    }

    #[test]
    fn parse_fid_invalid_returns_error() {
        assert!(matches!(parse_fid(""), Err(ParseFidError::Empty)));
        assert!(matches!(parse_fid("abc"), Err(ParseFidError::MissingComma)));
        assert!(matches!(parse_fid("3,"), Err(ParseFidError::EmptyKey)));
        assert!(matches!(parse_fid(",abc"), Err(ParseFidError::EmptyVolume)));
        assert!(matches!(
            parse_fid("abc,123456789"),
            Err(ParseFidError::InvalidVolume(_))
        ));
        assert!(matches!(parse_fid("3,12345678"), Err(ParseFidError::KeyTooShort)));
        assert!(matches!(parse_fid("3,1"), Err(ParseFidError::KeyTooShort)));
        assert!(matches!(
            parse_fid("3,zxcvbnm12345678"),
            Err(ParseFidError::InvalidKey(_))
        ));
        assert!(matches!(
            parse_fid("3,deadbeefzzzzzzzz"),
            Err(ParseFidError::InvalidCookie(_))
        ));
        assert!(matches!(
            parse_fid("3,0189b26a98_xyz"),
            Err(ParseFidError::InvalidDelta(_))
        ));
    }
}
