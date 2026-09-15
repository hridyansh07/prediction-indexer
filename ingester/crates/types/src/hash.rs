//! Finalized SHA-256 values and domain-separated hashing.
//!
//! [`Sha256`] represents any already-finalized SHA-256 as exactly 32 bytes. The
//! capture-specific named hashes below use
//! `SHA-256(domain ‖ len(8, big-endian) ‖ bytes)`. The domain tag and explicit
//! length prevent equal bytes in different roles from sharing an identity.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256 as Sha256Hasher};

pub type Digest32 = [u8; 32];

/// One finalized SHA-256 value. The raw bytes are authoritative; lowercase hex
/// is only its canonical wire representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256(Digest32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sha256Error {
    InvalidByteLength(usize),
    InvalidHexLength(usize),
    NonCanonicalHex,
}

impl Sha256 {
    pub const fn from_bytes(bytes: Digest32) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, Sha256Error> {
        let bytes: Digest32 = bytes
            .try_into()
            .map_err(|_| Sha256Error::InvalidByteLength(bytes.len()))?;
        Ok(Self(bytes))
    }

    pub fn from_hex(hex: &str) -> Result<Self, Sha256Error> {
        if hex.len() != 64 {
            return Err(Sha256Error::InvalidHexLength(hex.len()));
        }
        if !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Sha256Error::NonCanonicalHex);
        }
        let mut bytes = [0_u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let offset = index * 2;
            *byte = u8::from_str_radix(&hex[offset..offset + 2], 16)
                .expect("canonical lowercase hexadecimal was checked above");
        }
        Ok(Self(bytes))
    }

    /// Computes ordinary, non-domain-separated SHA-256 over `bytes`.
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256Hasher::digest(bytes).into())
    }

    pub const fn as_bytes(&self) -> &Digest32 {
        &self.0
    }

    /// Returns canonical lowercase hexadecimal. Formatting and serialization
    /// write directly from the raw bytes when an owned string is not needed.
    pub fn as_hex(&self) -> String {
        format!("{self}")
    }
}

impl fmt::Display for Sha256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Sha256Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidByteLength(length) => {
                write!(formatter, "SHA-256 must be 32 bytes, got {length}")
            }
            Self::InvalidHexLength(length) => {
                write!(formatter, "SHA-256 hex must be 64 bytes, got {length}")
            }
            Self::NonCanonicalHex => {
                formatter.write_str("SHA-256 hex must contain only lowercase hexadecimal")
            }
        }
    }
}

impl std::error::Error for Sha256Error {}

impl From<Digest32> for Sha256 {
    fn from(bytes: Digest32) -> Self {
        Self::from_bytes(bytes)
    }
}

impl TryFrom<&[u8]> for Sha256 {
    type Error = Sha256Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::from_slice(bytes)
    }
}

impl FromStr for Sha256 {
    type Err = Sha256Error;

    fn from_str(hex: &str) -> Result<Self, Self::Err> {
        Self::from_hex(hex)
    }
}

impl Serialize for Sha256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_hex(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

pub const DOMAIN_EVIDENCE: &str = "indexer.evidence.v1";
pub const DOMAIN_CONTENT: &str = "indexer.content.v1";
pub const DOMAIN_FACT: &str = "indexer.fact.v1";

/// Hashes bytes under a domain, streaming so a large payload never needs a second
/// copy in memory.
pub struct StreamHasher {
    inner: Sha256Hasher,
}

impl StreamHasher {
    pub fn new(domain: &str, length: usize) -> Self {
        let mut inner = Sha256Hasher::new();
        inner.update(domain.as_bytes());
        inner.update((length as u64).to_be_bytes());
        Self { inner }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }

    pub fn finish(self) -> Digest32 {
        self.inner.finalize().into()
    }

    pub fn hash(domain: &str, bytes: &[u8]) -> Digest32 {
        let mut hasher = Self::new(domain, bytes.len());
        hasher.update(bytes);
        hasher.finish()
    }
}

macro_rules! digest_newtype {
    ($name:ident, $domain:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Digest32);

        impl $name {
            pub fn hash(bytes: &[u8]) -> Self {
                Self(StreamHasher::hash($domain, bytes))
            }
            pub const fn from_raw(value: Digest32) -> Self {
                Self(value)
            }
            pub const fn as_bytes(&self) -> &Digest32 {
                &self.0
            }
            pub fn to_hex(&self) -> String {
                self.0.iter().map(|byte| format!("{byte:02x}")).collect()
            }
            pub fn from_hex(text: &str) -> Option<Self> {
                if text.len() != 64 {
                    return None;
                }
                let mut raw = [0u8; 32];
                for (index, slot) in raw.iter_mut().enumerate() {
                    *slot = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
                }
                Some(Self(raw))
            }
        }
    };
}

digest_newtype!(
    EvidenceHash,
    DOMAIN_EVIDENCE,
    "Hash of the exact delivered line, including its trailing newline."
);
digest_newtype!(
    ContentHash,
    DOMAIN_CONTENT,
    "Hash of a record's decoded payload. Identity is judged on this, not on the \
     transport line: two deliveries of the same fact with different whitespace are \
     one fact, whereas the same id with different content is a venue misbehaving."
);
digest_newtype!(
    FactHash,
    DOMAIN_FACT,
    "Hash of a committed fact's canonical bytes."
);

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::*;

    #[test]
    fn sha256_raw_hex_and_serde_round_trip_boundary_vectors() {
        for (raw, expected_hex) in [
            ([0_u8; 32], "0".repeat(64)),
            ([0xff_u8; 32], "f".repeat(64)),
        ] {
            let digest = Sha256::from(raw);
            let hex = digest.as_hex();
            assert_eq!(hex, expected_hex);
            assert_eq!(Sha256::from_slice(&raw), Ok(digest));
            assert_eq!(Sha256::from_hex(&hex), Ok(digest));
            assert_eq!(hex.parse::<Sha256>(), Ok(digest));
            assert_eq!(digest.as_bytes(), &raw);
            assert_eq!(
                serde_json::to_string(&digest).unwrap(),
                format!("\"{hex}\"")
            );
            assert_eq!(
                serde_json::from_str::<Sha256>(&format!("\"{hex}\"")).unwrap(),
                digest
            );
        }
    }

    #[test]
    fn sha256_rejects_noncanonical_or_wrong_length_inputs() {
        assert_eq!(
            Sha256::from_slice(&[0_u8; 31]),
            Err(Sha256Error::InvalidByteLength(31))
        );
        assert_eq!(
            Sha256::from_hex(&"a".repeat(63)),
            Err(Sha256Error::InvalidHexLength(63))
        );
        assert_eq!(
            Sha256::from_hex(&"A".repeat(64)),
            Err(Sha256Error::NonCanonicalHex)
        );
        assert_eq!(
            Sha256::from_hex(&"g".repeat(64)),
            Err(Sha256Error::NonCanonicalHex)
        );
        assert!(serde_json::from_str::<Sha256>(&format!("\"{}\"", "A".repeat(64))).is_err());
        assert!(serde_json::from_str::<Sha256>("[]").is_err());
    }

    #[test]
    fn sha256_equality_hash_and_order_are_bytewise() {
        let low = Sha256::from([0_u8; 32]);
        let mut middle_raw = [0_u8; 32];
        middle_raw[31] = 1;
        let middle = Sha256::from(middle_raw);
        let high = Sha256::from([0xff_u8; 32]);
        assert!(low < middle && middle < high);
        assert_eq!(HashSet::from([low, low, high]).len(), 2);
        assert_eq!(
            BTreeSet::from([high, low, middle])
                .into_iter()
                .collect::<Vec<_>>(),
            [low, middle, high]
        );
    }

    #[test]
    fn sha256_digest_is_a_final_value() {
        assert_eq!(
            Sha256::digest(b"abc").as_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn domains_separate_identical_bytes() {
        let bytes = b"same";
        assert_ne!(
            EvidenceHash::hash(bytes).as_bytes(),
            ContentHash::hash(bytes).as_bytes()
        );
    }

    #[test]
    fn hex_round_trips() {
        let digest = ContentHash::hash(b"payload");
        assert_eq!(ContentHash::from_hex(&digest.to_hex()), Some(digest));
        assert_eq!(ContentHash::from_hex("nope"), None);
    }

    #[test]
    fn length_prefix_prevents_concatenation_collisions() {
        // Without the length prefix, hashing "ab" then "c" and "a" then "bc"
        // would feed the digest identical bytes.
        let mut first = StreamHasher::new(DOMAIN_CONTENT, 3);
        first.update(b"ab");
        first.update(b"c");
        let mut second = StreamHasher::new(DOMAIN_CONTENT, 3);
        second.update(b"abc");
        assert_eq!(first.finish(), second.finish());
        assert_ne!(
            StreamHasher::hash(DOMAIN_CONTENT, b"abc"),
            StreamHasher::hash(DOMAIN_CONTENT, b"abcd")
        );
    }
}
