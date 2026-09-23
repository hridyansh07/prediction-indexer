//! Production cross-venue normalization for one canonical window.

use canonical_normalizer::{
    Normalization, Normalize, NormalizerConfigIdentity, NormalizerDescriptor, NormalizerError,
    VenueAdapter, normalize_with_adapter,
};
use indexer_finalize::JoinedCanonicalRecord;
use indexer_types::{EnvelopeView, Sha256, Venue};
use serde::{Deserialize, Serialize};

pub mod kalshi;
pub mod limitless;
pub mod polymarket;

const BUNDLE_DOMAIN: &[u8] = b"prediction-indexer/replay-normalizers/bundle/v1\0";
const CONFIG_DOMAIN: &[u8] = b"prediction-indexer/replay-normalizers/config/v1\0";

/// Complete typed semantic identity for the production normalizer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalNormalizerIdentity {
    pub identity_version: u16,
    pub venues: Vec<VenueNormalizerIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueNormalizerIdentity {
    pub venue: String,
    pub bundle_id: String,
    pub parser_version: u32,
    pub config: NormalizerConfigIdentity,
}

/// The only production `Normalize` implementation in this crate. Every
/// canonical record is routed unchanged to exactly one adapter by its envelope
/// venue; adapter state is never shared across venues.
pub struct CanonicalNormalizer {
    kalshi: kalshi::Kalshi,
    limitless: limitless::Limitless,
    polymarket: polymarket::Polymarket,
    identity: CanonicalNormalizerIdentity,
    descriptor: NormalizerDescriptor,
}

impl Default for CanonicalNormalizer {
    fn default() -> Self {
        Self::new(
            kalshi::Kalshi::default(),
            limitless::Limitless::default(),
            polymarket::Polymarket::default(),
        )
        .expect("default replay normalizer identity is serializable")
    }
}

impl CanonicalNormalizer {
    fn new(
        kalshi: kalshi::Kalshi,
        limitless: limitless::Limitless,
        polymarket: polymarket::Polymarket,
    ) -> Result<Self, NormalizerError> {
        // This order is part of the identity contract, independent of finish
        // order (Kalshi, Limitless, Polymarket).
        let identity = CanonicalNormalizerIdentity {
            identity_version: 1,
            venues: vec![
                venue_identity(&kalshi),
                venue_identity(&limitless),
                venue_identity(&polymarket),
            ],
        };
        let bundle_identity = identity
            .venues
            .iter()
            .map(|venue| VenueBundleIdentity {
                venue: &venue.venue,
                bundle_id: &venue.bundle_id,
                parser_version: venue.parser_version,
            })
            .collect::<Vec<_>>();
        let bundle_bytes = serde_json::to_vec(&bundle_identity).map_err(|error| {
            NormalizerError::new(format!(
                "composite bundle identity is not serializable: {error}"
            ))
        })?;
        let config_bytes = serde_json::to_vec(&identity).map_err(|error| {
            NormalizerError::new(format!(
                "composite config identity is not serializable: {error}"
            ))
        })?;
        Ok(Self {
            kalshi,
            limitless,
            polymarket,
            descriptor: NormalizerDescriptor {
                bundle_sha256: domain_digest(BUNDLE_DOMAIN, &bundle_bytes),
                config_sha256: domain_digest(CONFIG_DOMAIN, &config_bytes),
            },
            identity,
        })
    }

    pub const fn identity(&self) -> &CanonicalNormalizerIdentity {
        &self.identity
    }
}

#[derive(Serialize)]
struct VenueBundleIdentity<'a> {
    venue: &'a str,
    bundle_id: &'a str,
    parser_version: u32,
}

fn venue_identity<A: VenueAdapter>(adapter: &A) -> VenueNormalizerIdentity {
    VenueNormalizerIdentity {
        venue: A::VENUE.as_str().to_owned(),
        bundle_id: A::BUNDLE_ID.to_owned(),
        parser_version: A::PARSER_VERSION,
        config: adapter.config_identity(),
    }
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> Sha256 {
    let mut preimage = Vec::with_capacity(domain.len() + bytes.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(bytes);
    Sha256::digest(&preimage)
}

impl Normalize for CanonicalNormalizer {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }

    fn normalize(
        &mut self,
        source: &JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = EnvelopeView::parse(&source.envelope).map_err(|error| {
            NormalizerError::new(format!(
                "audited canonical envelope became invalid: {error}"
            ))
        })?;
        match envelope.venue {
            Venue::Kalshi => normalize_with_adapter(&mut self.kalshi, source),
            Venue::Limitless => normalize_with_adapter(&mut self.limitless, source),
            Venue::Polymarket => normalize_with_adapter(&mut self.polymarket, source),
            Venue::Internal => Ok(Normalization::Ignored {
                reason_code: "internal_venue".to_owned(),
            }),
        }
    }

    fn finish(&mut self) -> Result<(), NormalizerError> {
        finish_in_order(
            || self.kalshi.finish(),
            || self.limitless.finish(),
            || self.polymarket.finish(),
        )
    }
}

fn finish_in_order<K, L, P>(kalshi: K, limitless: L, polymarket: P) -> Result<(), NormalizerError>
where
    K: FnOnce() -> Result<(), NormalizerError>,
    L: FnOnce() -> Result<(), NormalizerError>,
    P: FnOnce() -> Result<(), NormalizerError>,
{
    let mut first = None;
    for result in [kalshi(), limitless(), polymarket()] {
        if first.is_none() {
            first = result.err();
        }
    }
    first.map_or(Ok(()), Err)
}

#[cfg(test)]
mod composite_tests {
    use super::*;
    use canonical_normalizer::Normalize;
    use indexer_finalize::{ContinuityVerdict, EventAddress};
    use indexer_types::ContentHash;
    use replay_domain::DecimalScale;
    use serde_json::json;
    use std::cell::RefCell;

    fn source(venue: &str, payload: serde_json::Value) -> JoinedCanonicalRecord {
        let payload = payload.to_string();
        let envelope = format!(
            "{}\n",
            json!({
                "envelope_version":2,"delivery_index":1,"record_id":"r",
                "visible_ns":1,"monotonic_ns":1,"venue":venue,
                "stream":"public_book","connection_epoch":"e","local_counter":1,
                "source_cursor":{"type":"unsequenced","counter":1},
                "kind":"venue_frame","raw_payload":payload,
            })
        )
        .into_bytes();
        JoinedCanonicalRecord {
            envelope,
            canonical_seq: 1,
            order_ns: 1,
            visible_ns: 1,
            visible_tie_group: None,
            event_address: EventAddress {
                canonical_seq: 1,
                lane_id: venue.to_owned(),
                delivery_index: 1,
            },
            record_id: "r".to_owned(),
            source_segment_sha256: Sha256::digest(b"source"),
            source_line_number: 1,
            content_hash: Sha256::from_bytes(*ContentHash::hash(payload.as_bytes()).as_bytes()),
            continuity: ContinuityVerdict::UnsequencedVenue,
        }
    }

    #[test]
    fn internal_is_visible_and_unknown_venue_is_deterministically_rejected() {
        let mut normalizer = CanonicalNormalizer::default();
        assert_eq!(
            normalizer
                .normalize(&source("internal", json!({"event":"audit"})))
                .unwrap(),
            Normalization::Ignored {
                reason_code: "internal_venue".to_owned()
            }
        );
        let error = normalizer
            .normalize(&source("unsupported", json!({})))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "audited canonical envelope became invalid: unknown venue: unsupported"
        );
    }

    #[test]
    fn identity_is_ordered_complete_and_additive_policy_changes_address_inputs() {
        let default = CanonicalNormalizer::default();
        assert_eq!(
            default
                .identity()
                .venues
                .iter()
                .map(|venue| venue.venue.as_str())
                .collect::<Vec<_>>(),
            ["kalshi", "limitless", "polymarket"]
        );
        assert_eq!(
            default
                .identity()
                .venues
                .iter()
                .map(|venue| venue.parser_version)
                .collect::<Vec<_>>(),
            [
                kalshi::PARSER_VERSION,
                limitless::PARSER_VERSION,
                polymarket::PARSER_VERSION
            ]
        );
        assert_eq!(
            default.descriptor().bundle_sha256.as_hex(),
            "8076a1e1156470b053856c4100f9f38f83a675bdb625383cb42c1d4e5603fb92"
        );
        assert_eq!(
            default.descriptor().config_sha256.as_hex(),
            "5a70988d6be21716850f50eecdf393733ca8d6b551896cfda4299cabca547d7f"
        );
        let changed = CanonicalNormalizer::new(
            kalshi::Kalshi::default(),
            limitless::Limitless::default(),
            polymarket::Polymarket::new(polymarket::Config {
                price_scale: DecimalScale::new(4).unwrap(),
                quantity_scale: DecimalScale::new(6).unwrap(),
                accept_additive_fields: false,
            }),
        )
        .unwrap();
        assert_ne!(default.descriptor(), changed.descriptor());
        assert_ne!(default.identity(), changed.identity());
    }

    #[test]
    fn finish_calls_every_adapter_in_required_order_and_returns_first_error() {
        let calls = RefCell::new(Vec::new());
        let result = finish_in_order(
            || {
                calls.borrow_mut().push("kalshi");
                Err(NormalizerError::new("first"))
            },
            || {
                calls.borrow_mut().push("limitless");
                Err(NormalizerError::new("second"))
            },
            || {
                calls.borrow_mut().push("polymarket");
                Ok(())
            },
        );
        assert_eq!(&*calls.borrow(), &["kalshi", "limitless", "polymarket"]);
        assert_eq!(result.unwrap_err().to_string(), "first");
    }
}
