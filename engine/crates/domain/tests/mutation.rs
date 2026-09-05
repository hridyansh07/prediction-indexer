use replay_domain::{PrepareError, Revision, Revisioned};

#[test]
fn rejected_preparation_and_stale_apply_never_partially_publish() {
    let mut state = Revisioned::new(vec![1_u8]);
    let rejected = state.prepare::<&str>(|_| Err("invalid change"));
    assert!(matches!(
        rejected,
        Err(PrepareError::Rejected("invalid change"))
    ));
    assert_eq!(state.value(), &[1]);
    assert_eq!(state.revision(), Revision::initial());

    let first = state
        .prepare::<()>(|current| {
            let mut next = current.clone();
            next.push(2);
            Ok(next)
        })
        .unwrap();
    let stale = state
        .prepare::<()>(|current| {
            let mut next = current.clone();
            next.push(3);
            Ok(next)
        })
        .unwrap();

    let receipt = state.apply(first).unwrap();
    assert_eq!(receipt.previous, Revision::initial());
    assert_eq!(receipt.current, Revision::new(1));
    let error = state.apply(stale).unwrap_err();
    assert_eq!(error.expected, Revision::new(1));
    assert_eq!(error.prepared_from, Revision::initial());
    assert_eq!(state.value(), &[1, 2]);
    assert_eq!(state.revision(), Revision::new(1));
}

#[test]
fn revision_overflow_fails_during_prepare() {
    let state = Revisioned::at_revision("book", Revision::new(u64::MAX));
    assert!(matches!(
        state.prepare::<()>(|_| Ok("replacement")),
        Err(PrepareError::RevisionOverflow)
    ));
    assert_eq!(state.value(), &"book");
}
