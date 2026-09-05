use replay_domain::{
    BookDelta, BookEvent, CanonicalProvenance, ContinuityVerdict, ContractOrientation,
    DecimalScale, DomainError, EventAddress, EventHeader, FullBook, InstrumentId, LaneId, Level,
    LevelSize, LevelSizeMode, Px, Qty, SegmentEvent, SegmentRecord, Side,
};

const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn scale(value: u8) -> DecimalScale {
    DecimalScale::new(value).unwrap()
}

fn record() -> SegmentRecord {
    let address = EventAddress::new(42, LaneId::new("lane-book-a").unwrap(), 9001, 3).unwrap();
    let provenance =
        CanonicalProvenance::new(DIGEST_A, 17, DIGEST_B, ContinuityVerdict::Continuous).unwrap();
    let header = EventHeader::new(
        1_785_409_600_000_000_000,
        1_785_409_600_000_000_000,
        Some(7),
        address,
        "record-42",
        provenance,
    )
    .unwrap();
    let delta = BookDelta::new(
        InstrumentId::new("venue:market-yes").unwrap(),
        ContractOrientation::Complement,
        Side::Bid,
        Px::parse("0.5100", scale(4)).unwrap(),
        LevelSize::relative(Qty::parse("-2.50", scale(2)).unwrap()).unwrap(),
        Some("venue-book-hash".to_owned()),
    )
    .unwrap();
    SegmentRecord::new(header, SegmentEvent::Book(BookEvent::Delta(delta))).unwrap()
}

#[test]
fn canonical_json_matches_the_golden_vector_and_round_trips() {
    let expected = concat!(
        "{\"schema_version\":1,\"header\":{\"order_ns\":1785409600000000000,",
        "\"visible_ns\":1785409600000000000,\"visible_tie_group\":7,",
        "\"address\":{\"canonical_seq\":42,\"lane\":\"lane-book-a\",",
        "\"delivery_index\":9001,\"event_index\":3},\"record_id\":\"record-42\",",
        "\"provenance\":{\"source_segment_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
        "\"source_line_number\":17,\"content_hash\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",",
        "\"continuity\":\"continuous\"}},\"event\":{\"kind\":\"book\",",
        "\"value\":{\"kind\":\"delta\",\"value\":{",
        "\"instrument\":\"venue:market-yes\",\"orientation\":\"complement\",",
        "\"side\":\"bid\",\"price\":{\"atoms\":5100,\"scale\":4,",
        "\"unit\":\"quote_per_contract\"},\"size\":{\"mode\":\"relative\",",
        "\"quantity\":{\"atoms\":-250,\"scale\":2,\"unit\":\"contracts\"}},",
        "\"book_hash\":\"venue-book-hash\"}}}}"
    );
    assert_eq!(record().to_canonical_json(), expected.as_bytes());
    assert_eq!(
        SegmentRecord::from_canonical_json(expected.as_bytes()).unwrap(),
        record()
    );
}

#[test]
fn provenance_tie_and_address_survive_serialization() {
    let decoded = SegmentRecord::from_canonical_json(&record().to_canonical_json()).unwrap();
    let header = decoded.header();
    assert_eq!(header.order_ns(), header.visible_ns());
    assert_eq!(header.visible_tie_group(), Some(7));
    assert_eq!(header.record_id(), "record-42");
    assert_eq!(header.address().canonical_seq(), 42);
    assert_eq!(header.address().lane().as_str(), "lane-book-a");
    assert_eq!(header.address().delivery_index(), 9001);
    assert_eq!(header.address().event_index(), 3);
    assert_eq!(header.provenance().source_segment_sha256(), DIGEST_A);
    assert_eq!(header.provenance().source_line_number(), 17);
    assert_eq!(header.provenance().content_hash(), DIGEST_B);
    assert_eq!(
        header.provenance().continuity(),
        ContinuityVerdict::Continuous
    );
}

#[test]
fn child_event_indexes_are_zero_based_and_preserve_the_delivery_address() {
    let parent = EventAddress::new(5, LaneId::new("lane-a").unwrap(), 81, 0).unwrap();
    let children: Vec<_> = (0..3).map(|index| parent.child(index)).collect();
    assert_eq!(
        children
            .iter()
            .map(EventAddress::event_index)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(children.iter().all(|child| child.canonical_seq() == 5));
    assert!(children.iter().all(|child| child.delivery_index() == 81));
    assert!(children.iter().all(|child| child.lane() == parent.lane()));
}

#[test]
fn unknown_fields_variants_and_versions_are_rejected() {
    let canonical = String::from_utf8(record().to_canonical_json()).unwrap();
    let cases = [
        canonical.replacen(
            "{\"schema_version\":1",
            "{\"unknown\":0,\"schema_version\":1",
            1,
        ),
        canonical.replacen("\"side\":\"bid\"", "\"side\":\"offer\"", 1),
        canonical.replacen("\"kind\":\"delta\"", "\"kind\":\"replace\"", 1),
        canonical.replacen("\"atoms\":5100", "\"atoms\":5100,\"float\":0.51", 1),
    ];
    for invalid in cases {
        assert!(SegmentRecord::from_canonical_json(invalid.as_bytes()).is_err());
    }
    let future = canonical.replacen("\"schema_version\":1", "\"schema_version\":2", 1);
    assert_eq!(
        SegmentRecord::from_canonical_json(future.as_bytes()),
        Err(DomainError::UnsupportedSchemaVersion(2))
    );
}

#[test]
fn alternate_json_spelling_is_not_canonical() {
    let with_newline = [record().to_canonical_json(), b"\n".to_vec()].concat();
    assert_eq!(
        SegmentRecord::from_canonical_json(&with_newline),
        Err(DomainError::NonCanonicalEncoding)
    );
}

#[test]
fn malformed_financial_states_are_rejected_on_decode() {
    let canonical = String::from_utf8(record().to_canonical_json()).unwrap();
    let negative_price = canonical.replacen("\"atoms\":5100", "\"atoms\":-1", 1);
    assert!(SegmentRecord::from_canonical_json(negative_price.as_bytes()).is_err());
    let invalid_relative = canonical.replacen("\"atoms\":-250", "\"atoms\":0", 1);
    assert!(SegmentRecord::from_canonical_json(invalid_relative.as_bytes()).is_err());
}

#[test]
fn side_orientation_and_absolute_relative_semantics_stay_distinct() {
    let decoded = SegmentRecord::from_canonical_json(&record().to_canonical_json()).unwrap();
    let SegmentEvent::Book(BookEvent::Delta(delta)) = decoded.event() else {
        panic!("expected delta")
    };
    assert_eq!(delta.side(), Side::Bid);
    assert_eq!(delta.orientation(), ContractOrientation::Complement);
    assert_eq!(delta.size().mode(), LevelSizeMode::Relative);
    assert_eq!(delta.size().quantity().atoms(), -250);
    assert!(LevelSize::absolute(Qty::from_atoms(-1, scale(0))).is_err());
    assert!(LevelSize::absolute(Qty::from_atoms(0, scale(0))).is_ok());
    assert!(LevelSize::relative(Qty::from_atoms(0, scale(0))).is_err());
}

#[test]
fn level_size_golden_vectors_round_trip() {
    let absolute = LevelSize::absolute(Qty::from_atoms(0, scale(3))).unwrap();
    let relative = LevelSize::relative(Qty::from_atoms(-25, scale(2))).unwrap();
    let vectors = [
        (
            absolute,
            r#"{"mode":"absolute","quantity":{"atoms":0,"scale":3,"unit":"contracts"}}"#,
        ),
        (
            relative,
            r#"{"mode":"relative","quantity":{"atoms":-25,"scale":2,"unit":"contracts"}}"#,
        ),
    ];
    for (value, expected) in vectors {
        assert_eq!(serde_json::to_string(&value).unwrap(), expected);
        assert_eq!(serde_json::from_str::<LevelSize>(expected).unwrap(), value);
    }
    assert!(
        serde_json::from_str::<LevelSize>(
            r#"{"mode":"relative","quantity":{"atoms":0,"scale":2,"unit":"contracts"}}"#,
        )
        .is_err()
    );
}

#[test]
fn full_books_sort_sides_and_reject_duplicate_or_nonpositive_levels() {
    let px = |atoms| Px::from_atoms(atoms, scale(2)).unwrap();
    let qty = |atoms| Qty::from_atoms(atoms, scale(0));
    let book = FullBook::new(
        InstrumentId::new("venue:asset").unwrap(),
        ContractOrientation::Outcome,
        vec![
            Level::new(px(40), qty(2)).unwrap(),
            Level::new(px(50), qty(1)).unwrap(),
        ],
        vec![
            Level::new(px(70), qty(1)).unwrap(),
            Level::new(px(60), qty(2)).unwrap(),
        ],
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        book.bids()
            .iter()
            .map(|level| level.price().atoms())
            .collect::<Vec<_>>(),
        [50, 40]
    );
    assert_eq!(
        book.asks()
            .iter()
            .map(|level| level.price().atoms())
            .collect::<Vec<_>>(),
        [60, 70]
    );
    assert!(Level::new(px(50), qty(0)).is_err());
    assert!(
        FullBook::new(
            InstrumentId::new("venue:asset").unwrap(),
            ContractOrientation::Outcome,
            vec![
                Level::new(px(50), qty(1)).unwrap(),
                Level::new(px(50), qty(2)).unwrap()
            ],
            vec![],
            None,
            None,
        )
        .is_err()
    );
}
