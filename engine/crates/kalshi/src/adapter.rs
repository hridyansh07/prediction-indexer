use canonical_normalizer::{
    CanonicalEnvelope, Normalization, NormalizerConfigIdentity, NormalizerError, VenueAdapter,
};
use indexer_types::{RecordKind, SourceCursor, Stream, Venue};
use serde_json::Value;

use crate::{
    Config, NORMALIZER_BUNDLE_ID, PARSER_VERSION,
    message::{MessageOutcome, normalize_message},
    process::{ProcessOutcome, normalize_process},
};

/// Kalshi's venue-specific extension. Canonical-envelope handling, descriptor
/// construction, and lifecycle are inherited from `canonical_normalizer::Normalizer`.
pub struct Kalshi {
    config: Config,
}

impl From<Config> for Kalshi {
    fn from(config: Config) -> Self {
        Self { config }
    }
}

impl Default for Kalshi {
    fn default() -> Self {
        Self::from(Config::default())
    }
}

impl VenueAdapter for Kalshi {
    const VENUE: Venue = Venue::Kalshi;
    const PARSER_VERSION: u32 = PARSER_VERSION;
    const BUNDLE_ID: &'static str = NORMALIZER_BUNDLE_ID;

    fn config_identity(&self) -> NormalizerConfigIdentity {
        self.config.identity()
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
        let mut ignored = Vec::new();
        for value in messages {
            match normalize_message(envelope, value, self.config, batched) {
                Ok(MessageOutcome::Events(mut children)) => events.append(&mut children),
                Ok(MessageOutcome::Ignored(reason)) => {
                    if !ignored.contains(&reason) {
                        ignored.push(reason);
                    }
                }
                Err(reject) => {
                    return Ok(input.reject(
                        PARSER_VERSION,
                        reject.code,
                        if batched { None } else { reject.instrument },
                    ));
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
