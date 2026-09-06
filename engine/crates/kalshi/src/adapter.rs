use std::collections::BTreeSet;

use canonical_normalizer::{CanonicalEnvelope, Normalization, NormalizerError, VenueAdapter};
use indexer_types::{RecordKind, SourceCursor, Stream, Venue};
use serde_json::Value;

use crate::{
    Config, NORMALIZER_BUNDLE_ID, PARSER_VERSION,
    message::{MessageOutcome, ProcessOutcome, normalize_message, normalize_process},
};

/// Kalshi's venue-specific extension. Canonical-envelope handling, descriptor
/// construction, and lifecycle are inherited from `canonical_normalizer::Normalizer`.
pub struct Kalshi {
    config: Config,
}

impl TryFrom<Config> for Kalshi {
    type Error = NormalizerError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        if config.use_yes_price {
            return Err(NormalizerError::new(
                "Kalshi v1 does not support use_yes_price=true captures",
            ));
        }
        Ok(Self { config })
    }
}

impl Default for Kalshi {
    fn default() -> Self {
        Self::try_from(Config::default()).expect("default Kalshi config is supported")
    }
}

impl VenueAdapter for Kalshi {
    type ConfigIdentity = Config;

    const VENUE: Venue = Venue::Kalshi;
    const PARSER_VERSION: u32 = PARSER_VERSION;
    const BUNDLE_ID: &'static str = NORMALIZER_BUNDLE_ID;

    fn config_identity(&self) -> Self::ConfigIdentity {
        self.config
    }

    fn normalize(
        &mut self,
        input: CanonicalEnvelope<'_>,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = input.envelope();
        let payload = input.payload();
        if envelope.stream == Stream::Process {
            return Ok(match normalize_process(envelope, payload) {
                Ok(ProcessOutcome::Event(event)) => Normalization::Events(vec![event]),
                Ok(ProcessOutcome::Ignored(reason)) => Normalization::Ignored {
                    reason_code: reason.to_owned(),
                },
                Err(code) => input.reject(PARSER_VERSION, code, None),
            });
        }
        if envelope.kind != RecordKind::VenueFrame {
            return Ok(input.reject(PARSER_VERSION, "unexpected_record_kind", None));
        }

        let (messages, batched) = match payload {
            Value::Array(values) => (values.as_slice(), true),
            value => (std::slice::from_ref(value), false),
        };
        if batched
            && !matches!(
                envelope.source_cursor,
                Some(SourceCursor::Unsequenced { .. })
            )
        {
            return Ok(input.reject(PARSER_VERSION, "batch_cursor_mismatch", None));
        }
        if messages.is_empty() {
            return Ok(Normalization::Events(Vec::new()));
        }
        let mut events = Vec::new();
        let mut ignored = BTreeSet::new();
        for value in messages {
            match normalize_message(envelope, value, self.config, batched) {
                Ok(MessageOutcome::Events(mut children)) => events.append(&mut children),
                Ok(MessageOutcome::Ignored(reason)) => {
                    ignored.insert(reason);
                }
                Err(reject) => {
                    return Ok(input.reject(PARSER_VERSION, reject.code, reject.instrument));
                }
            }
        }
        if events.is_empty() && !ignored.is_empty() {
            Ok(Normalization::Ignored {
                reason_code: if ignored.len() == 1 {
                    ignored.into_iter().next().expect("non-empty").to_owned()
                } else {
                    "supported_non_domain_messages".to_owned()
                },
            })
        } else {
            Ok(Normalization::Events(events))
        }
    }
}
