//! Domain-separated hashing.
//!
//! Every digest is `SHA-256(domain ‖ len(8, big-endian) ‖ bytes)`. The domain tag
//! and explicit length mean two values in different roles cannot collide even when
//! their bytes coincide — without them, a raw line that happened to equal a
//! canonical fact encoding would hash identically, and the store's integrity check
//! would accept one where it expected the other.

use sha2::{Digest, Sha256};

pub type Digest32 = [u8; 32];

pub const DOMAIN_EVIDENCE: &str = "indexer.evidence.v1";
pub const DOMAIN_CONTENT: &str = "indexer.content.v1";
pub const DOMAIN_FACT: &str = "indexer.fact.v1";

/// Hashes bytes under a domain, streaming so a large payload never needs a second
/// copy in memory.
pub struct StreamHasher {
    inner: Sha256,
}

impl StreamHasher {
    pub fn new(domain: &str, length: usize) -> Self {
        let mut inner = Sha256::new();
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
    use super::*;

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
