use core::fmt;

use serde::{Deserialize, Serialize};

/// A canonical lowercase SHA-1 digest.
///
/// This is intentionally distinct from `indexer_types::Sha256`: Polymarket's
/// server-compatible full-book state hash is SHA-1, not a source identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha1([u8; 20]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sha1Error {
    InvalidHexLength(usize),
    NonCanonicalHex,
}

impl Sha1 {
    pub fn from_hex(hex: &str) -> Result<Self, Sha1Error> {
        if hex.len() != 40 {
            return Err(Sha1Error::InvalidHexLength(hex.len()));
        }
        let mut bytes = [0_u8; 20];
        for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_nibble(pair[0]).ok_or(Sha1Error::NonCanonicalHex)?;
            let low = decode_nibble(pair[1]).ok_or(Sha1Error::NonCanonicalHex)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    pub fn as_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(40);
        for byte in self.0 {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        output
    }
}

impl fmt::Display for Sha1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_hex())
    }
}

impl fmt::Display for Sha1Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHexLength(length) => {
                write!(
                    formatter,
                    "SHA-1 hex must contain 40 characters, got {length}"
                )
            }
            Self::NonCanonicalHex => formatter.write_str("SHA-1 hex must be lowercase ASCII hex"),
        }
    }
}

impl std::error::Error for Sha1Error {}

impl Serialize for Sha1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.as_hex())
    }
}

impl<'de> Deserialize<'de> for Sha1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::from_hex(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Algorithm-explicit full-book state evidence supplied by a venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "algorithm", content = "digest", rename_all = "snake_case")]
pub enum BookStateHash {
    Sha1(Sha1),
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
