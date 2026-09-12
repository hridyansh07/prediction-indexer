use replay_domain::InstrumentId;

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
