use canonical_normalizer::{
    CanonicalEnvelope, Normalization, NormalizerConfigIdentity, NormalizerError, VenueAdapter,
};
use indexer_finalize::JoinedCanonicalRecord;
use indexer_types::{EnvelopeView, RecordKind, Stream, Venue};
use serde_json::Value;

use crate::{
    Config, NORMALIZER_BUNDLE_ID, PARSER_VERSION,
    message::{
        MessageOutcome, ProcessOutcome, normalize_message, normalize_process,
        normalize_rest_snapshot,
    },
};

pub struct Polymarket {
    config: Config,
}

impl Polymarket {
    pub const fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Default for Polymarket {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl VenueAdapter for Polymarket {
    const VENUE: Venue = Venue::Polymarket;
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
        if envelope.stream == Stream::Process {
            return Ok(match normalize_process(envelope, input.payload()) {
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
        if envelope.stream == Stream::PublicSnapshot {
            return Ok(outcome(
                &input,
                normalize_rest_snapshot(envelope, input.payload(), self.config),
                false,
            ));
        }
        if envelope.stream != Stream::PublicBook {
            return Ok(input.reject(PARSER_VERSION, "message_stream_mismatch", None));
        }
        let (messages, batched) = match input.payload() {
            Value::Array(values) => (values.as_slice(), true),
            value => (std::slice::from_ref(value), false),
        };
        if messages.is_empty() {
            return Ok(Normalization::Events(Vec::new()));
        }
        let mut events = Vec::new();
        for message in messages {
            match normalize_message(envelope, message, self.config) {
                Ok(MessageOutcome::Events(mut children)) => events.append(&mut children),
                Err(reject) => {
                    return Ok(input.reject(
                        PARSER_VERSION,
                        reject.code,
                        if batched || is_multi_change_batch(message) {
                            None
                        } else {
                            reject.instrument
                        },
                    ));
                }
            }
        }
        Ok(Normalization::Events(events))
    }

    fn normalize_non_json(
        &mut self,
        _source: &JoinedCanonicalRecord,
        envelope: &EnvelopeView<'_>,
    ) -> Result<Option<Normalization>, NormalizerError> {
        Ok((envelope.stream == Stream::PublicBook
            && envelope.kind == RecordKind::VenueFrame
            && envelope.raw_payload == "PONG")
            .then(|| Normalization::Ignored {
                reason_code: "application_heartbeat".to_owned(),
            }))
    }
}

fn is_multi_change_batch(message: &Value) -> bool {
    message
        .as_object()
        .filter(|object| object.get("event_type").and_then(Value::as_str) == Some("price_change"))
        .and_then(|object| object.get("price_changes"))
        .and_then(Value::as_array)
        .is_some_and(|changes| changes.len() > 1)
}

fn outcome(
    input: &CanonicalEnvelope<'_>,
    result: Result<MessageOutcome, crate::error::Reject>,
    batched: bool,
) -> Normalization {
    match result {
        Ok(MessageOutcome::Events(events)) => Normalization::Events(events),
        Err(reject) => input.reject(
            PARSER_VERSION,
            reject.code,
            if batched { None } else { reject.instrument },
        ),
    }
}
