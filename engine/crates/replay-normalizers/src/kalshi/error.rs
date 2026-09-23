use replay_domain::InstrumentId;

impl From<canonical_normalizer::ObjectError> for Reject {
    fn from(error: canonical_normalizer::ObjectError) -> Self {
        Self::new(match error {
            canonical_normalizer::ObjectError::UnknownField => "unknown_field",
            canonical_normalizer::ObjectError::MissingRequiredField => "missing_required_field",
        })
    }
}

#[derive(Debug)]
pub(crate) struct Reject {
    pub(crate) code: &'static str,
    pub(crate) instrument: Option<InstrumentId>,
}

impl Reject {
    pub(crate) const fn new(code: &'static str) -> Self {
        Self {
            code,
            instrument: None,
        }
    }

    pub(crate) fn for_instrument(code: &'static str, instrument: InstrumentId) -> Self {
        Self {
            code,
            instrument: Some(instrument),
        }
    }
}
