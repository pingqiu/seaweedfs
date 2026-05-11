//! Parse a SeaweedFS fid into `(volume_id, needle_id, cookie)`.
//!
//! SeaweedFS fids use:
//!
//! ```text
//! <volume_id>,<needle_id_hex><cookie_hex>[_<delta>]
//! ```
//!
//! This is a thin typed wrapper over the Rust volume server's canonical
//! [`FileId`] parser. Keep it that way so RDMA accepts exactly the same
//! fid grammar as the regular volume APIs.

use thiserror::Error;

use crate::storage::needle::needle::FileId;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseFidError {
    #[error("fid is empty")]
    Empty,
    #[error("fid missing comma or slash separator")]
    MissingSeparator,
    #[error("fid has empty volume_id half")]
    EmptyVolume,
    #[error("fid has empty key half")]
    EmptyKey,
    #[error("invalid fid: {0}")]
    InvalidFileId(String),
}

pub fn parse_fid(s: &str) -> Result<(u32, u64, u32), ParseFidError> {
    if s.is_empty() {
        return Err(ParseFidError::Empty);
    }

    let sep = s
        .find(',')
        .or_else(|| s.find('/'))
        .ok_or(ParseFidError::MissingSeparator)?;
    let (vid_part, rest) = (&s[..sep], &s[sep + 1..]);
    if vid_part.is_empty() {
        return Err(ParseFidError::EmptyVolume);
    }
    if rest.is_empty() {
        return Err(ParseFidError::EmptyKey);
    }

    let fid = FileId::parse(s).map_err(ParseFidError::InvalidFileId)?;
    Ok((fid.volume_id.0, fid.key.0, fid.cookie.0))
}

#[cfg(test)]
mod tests {
    use super::{parse_fid, ParseFidError};

    #[test]
    fn parse_fid_extracts_volume_key_and_cookie() {
        assert_eq!(parse_fid("3,0189b26a98").unwrap(), (3, 0x01, 0x89b26a98));
        assert_eq!(parse_fid("3,0a12345678").unwrap(), (3, 0x0a, 0x12345678));
        assert_eq!(
            parse_fid("3,deadbeef89abcdef").unwrap(),
            (3, 0xdeadbeef, 0x89abcdef)
        );
        assert_eq!(parse_fid("12345,0abc01234567").unwrap().0, 12345);
        assert_eq!(parse_fid("3,000189b26a98").unwrap().1, 0x0001);
    }

    #[test]
    fn parse_fid_applies_delta_suffix() {
        assert_eq!(parse_fid("3,0189b26a98_5").unwrap(), (3, 0x06, 0x89b26a98));
    }

    #[test]
    fn parse_fid_invalid_returns_error() {
        assert!(matches!(parse_fid(""), Err(ParseFidError::Empty)));
        assert!(matches!(
            parse_fid("abc"),
            Err(ParseFidError::MissingSeparator)
        ));
        assert!(matches!(parse_fid("3,"), Err(ParseFidError::EmptyKey)));
        assert!(matches!(parse_fid(",abc"), Err(ParseFidError::EmptyVolume)));
        assert!(matches!(
            parse_fid("abc,123456789"),
            Err(ParseFidError::InvalidFileId(_))
        ));
        assert!(matches!(
            parse_fid("3,12345678"),
            Err(ParseFidError::InvalidFileId(_))
        ));
        assert!(matches!(
            parse_fid("3,1"),
            Err(ParseFidError::InvalidFileId(_))
        ));
        assert!(matches!(
            parse_fid("3,zxcvbnm12345678"),
            Err(ParseFidError::InvalidFileId(_))
        ));
        assert!(matches!(
            parse_fid("3,deadbeefzzzzzzzz"),
            Err(ParseFidError::InvalidFileId(_))
        ));
        assert!(matches!(
            parse_fid("3,0189b26a98_xyz"),
            Err(ParseFidError::InvalidFileId(_))
        ));
    }

    #[test]
    fn parse_fid_accepts_slash_separator_like_volume_api() {
        assert_eq!(parse_fid("3/0189b26a98").unwrap(), (3, 0x01, 0x89b26a98));
    }

    #[test]
    fn parse_fid_rejects_overlong_key_hash_like_volume_api() {
        assert!(matches!(
            parse_fid("3,123456789abcdef0123456789"),
            Err(ParseFidError::InvalidFileId(_))
        ));
    }
}
