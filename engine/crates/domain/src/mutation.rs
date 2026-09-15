use core::fmt;

/// Monotonic state revision used only to reject stale prepared mutations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn initial() -> Self {
        Self(0)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrepareError<E> {
    Rejected(E),
    RevisionOverflow,
}

/// Complete, owned replacement prepared against exactly one base revision.
/// It is intentionally not `Clone` and is consumed by publication.
#[derive(Debug)]
pub struct PreparedMutation<T> {
    prepared_from: Revision,
    publish_at: Revision,
    replacement: T,
}

impl<T> PreparedMutation<T> {
    pub const fn prepared_from(&self) -> Revision {
        self.prepared_from
    }

    pub const fn publish_at(&self) -> Revision {
        self.publish_at
    }
}

#[derive(Debug)]
pub struct ApplyError<T> {
    pub expected: Revision,
    pub prepared_from: Revision,
    pub mutation: PreparedMutation<T>,
}

impl<T> fmt::Display for ApplyError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "stale mutation: current revision {}, prepared from {}",
            self.expected.get(),
            self.prepared_from.get()
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationReceipt {
    pub previous: Revision,
    pub current: Revision,
}

/// Generic atomic publication boundary for future book state.
///
/// Preparation performs every fallible domain operation against `&T` and owns a
/// complete replacement. Apply performs the stale check before its sole write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Revisioned<T> {
    revision: Revision,
    value: T,
}

impl<T> Revisioned<T> {
    pub const fn new(value: T) -> Self {
        Self::at_revision(value, Revision::initial())
    }

    pub const fn at_revision(value: T, revision: Revision) -> Self {
        Self { revision, value }
    }

    pub const fn revision(&self) -> Revision {
        self.revision
    }

    pub const fn value(&self) -> &T {
        &self.value
    }

    pub fn prepare<E>(
        &self,
        build: impl FnOnce(&T) -> Result<T, E>,
    ) -> Result<PreparedMutation<T>, PrepareError<E>> {
        let publish_at = self
            .revision
            .0
            .checked_add(1)
            .map(Revision)
            .ok_or(PrepareError::RevisionOverflow)?;
        let replacement = build(&self.value).map_err(PrepareError::Rejected)?;
        Ok(PreparedMutation {
            prepared_from: self.revision,
            publish_at,
            replacement,
        })
    }

    pub fn apply(
        &mut self,
        mutation: PreparedMutation<T>,
    ) -> Result<MutationReceipt, ApplyError<T>> {
        if mutation.prepared_from != self.revision {
            return Err(ApplyError {
                expected: self.revision,
                prepared_from: mutation.prepared_from,
                mutation,
            });
        }
        let previous = self.revision;
        self.value = mutation.replacement;
        self.revision = mutation.publish_at;
        Ok(MutationReceipt {
            previous,
            current: self.revision,
        })
    }
}
