use canonical_normalizer::{
    CanonicalEnvelope, Normalization, NormalizerConfigIdentity, NormalizerError, VenueAdapter,
};
use indexer_types::{RecordKind, Stream, Venue};

use crate::{
    Config, NORMALIZER_BUNDLE_ID, PARSER_VERSION,
    message::{MessageOutcome, ProcessOutcome, normalize_message, normalize_process},
};

/// Limitless's venue-specific extension. The generic normalizer retains sole
/// ownership of canonical decoding, provenance, identities, and lifecycle.
pub struct Limitless {
    config: Config,
}

impl TryFrom<Config> for Limitless {
    type Error = NormalizerError;

    fn try_from(config: Config) -> Result<Self, Self::Error> {
        if config.price_scale.exponent() < 3 {
            return Err(NormalizerError::new(
                "Limitless price scale must represent the venue's 0.001 tick",
            ));
        }
        Ok(Self { config })
    }
}

impl Default for Limitless {
    fn default() -> Self {
        Self::try_from(Config::default()).expect("default Limitless config is supported")
    }
}

impl VenueAdapter for Limitless {
    const VENUE: Venue = Venue::Limitless;
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
        if envelope.stream != Stream::PublicBook {
            return Ok(input.reject(PARSER_VERSION, "message_stream_mismatch", None));
        }
        Ok(
            match normalize_message(envelope, input.payload(), self.config) {
                Ok(MessageOutcome::Events(events)) => Normalization::Events(events),
                Ok(MessageOutcome::Ignored(reason)) => Normalization::Ignored {
                    reason_code: reason.to_owned(),
                },
                Err(reject) => input.reject(PARSER_VERSION, reject.code, reject.instrument),
            },
        )
    }
}
