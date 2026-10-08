use replay_domain::{
    AuditAnchor, BookDelta, BookEvent, BookStateHash, CanonicalProvenance, ConditionalMarketPrice,
    ContinuityVerdict, ContractOrientation, DecimalScale, DomainError, EventAddress, EventHeader,
    FaultImpact, FullBook, InstrumentId, LaneId, Level, LevelChange, NormalizationFault,
    PositiveQty, Qty, SegmentEvent, SegmentRecord, Sha1, Side,
};

const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn scale(value: u8) -> DecimalScale {
    DecimalScale::new(value).unwrap()
}

#[test]
fn book_keys_preserve_orientation_and_token_identity() {
    let mut books = std::collections::HashMap::new();
    for (id, orientation, atoms) in [
        ("kalshi:TEST", ContractOrientation::Outcome, 41),
        ("kalshi:TEST", ContractOrientation::Complement, 56),
        ("polymarket:17", ContractOrientation::Outcome, 37),
        ("polymarket:29", ContractOrientation::Outcome, 62),
    ] {
        let book = FullBook::new(
            InstrumentId::new(id).unwrap(),
            orientation,
            vec![Level::new(
                ConditionalMarketPrice::from_atoms(atoms, scale(2)).unwrap(),
                PositiveQty::parse("3", scale(0)).unwrap(),
            )],
            vec![],
            None,
            None,
        )
        .unwrap();
        let key = book.book_key();
        assert_eq!(key.instrument.as_str(), id);
        assert_eq!(key.orientation, orientation);
        assert!(books.insert(key, book).is_none());
    }
    assert_eq!(books.len(), 4);
    for (orientation, atoms) in [
        (ContractOrientation::Outcome, 41),
        (ContractOrientation::Complement, 56),
    ] {
        let delta = BookDelta::new(
            InstrumentId::new("kalshi:TEST").unwrap(),
            orientation,
            Side::Bid,
            ConditionalMarketPrice::from_atoms(atoms, scale(2)).unwrap(),
            LevelChange::Delete,
            None,
        )
        .unwrap();
        assert_eq!(books[&delta.book_key()].bids()[0].price().atoms(), atoms);
        let anchor = AuditAnchor::new(
            delta.instrument().clone(),
            orientation,
            vec![],
            vec![],
            BookStateHash::Sha1(Sha1::from_hex(&"a".repeat(40)).unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(anchor.book_key(), delta.book_key());
    }
}

fn record() -> SegmentRecord {
    let address = EventAddress::new(42, LaneId::new("lane-book-a").unwrap(), 9001, 3).unwrap();
    let provenance = CanonicalProvenance::new(
        DIGEST_A.parse().unwrap(),
        17,
        DIGEST_B.parse().unwrap(),
        ContinuityVerdict::Continuous,
    )
    .unwrap();
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
        ConditionalMarketPrice::parse("0.5100", scale(4)).unwrap(),
        LevelChange::Decrease(PositiveQty::parse("2.50", scale(2)).unwrap()),
        Some(BookStateHash::Sha1(
            Sha1::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap(),
        )),
    )
    .unwrap();
    SegmentRecord::new(header, SegmentEvent::Book(BookEvent::Delta(delta))).unwrap()
}

#[test]
fn canonical_json_matches_the_golden_vector_and_round_trips() {
    let expected = concat!(
        "{\"schema_version\":4,\"header\":{\"order_ns\":1785409600000000000,",
        "\"visible_ns\":1785409600000000000,\"visible_tie_group\":7,",
        "\"address\":{\"canonical_seq\":42,\"lane\":\"lane-book-a\",",
        "\"delivery_index\":9001,\"event_index\":3},\"record_id\":\"record-42\",",
        "\"provenance\":{\"source_segment_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
        "\"source_line_number\":17,\"content_hash\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",",
        "\"continuity\":\"continuous\"}},\"event\":{\"kind\":\"book\",",
        "\"value\":{\"kind\":\"delta\",\"value\":{",
        "\"instrument\":\"venue:market-yes\",\"orientation\":\"complement\",",
        "\"side\":\"bid\",\"price\":{\"atoms\":5100,\"scale\":4,",
        "\"unit\":\"quote_per_contract\"},\"change\":{\"kind\":\"decrease\",",
        "\"value\":{\"atoms\":250,\"scale\":2,\"unit\":\"contracts\"}},",
        "\"book_hash\":{\"algorithm\":\"sha1\",\"digest\":\"0123456789abcdef0123456789abcdef01234567\"},\"venue_time\":null}}}}"
    );
    assert_eq!(record().to_canonical_json(), expected.as_bytes());
    assert_eq!(
        SegmentRecord::from_canonical_json(expected.as_bytes()).unwrap(),
        record()
    );
}

#[test]
fn schema3_book_and_trade_records_preserve_their_closed_canonical_bytes() {
    let original = record();
    let events = [
        original.event().clone(),
        SegmentEvent::Book(BookEvent::Full(
            FullBook::new(
                InstrumentId::new("kalshi:OLD").unwrap(),
                ContractOrientation::Outcome,
                vec![],
                vec![],
                None,
                None,
            )
            .unwrap(),
        )),
        SegmentEvent::Trade(replay_domain::TradeEvent::new(
            InstrumentId::new("kalshi:OLD").unwrap(),
            ContractOrientation::Outcome,
            ConditionalMarketPrice::parse("0.5", scale(1)).unwrap(),
            PositiveQty::parse("1", scale(0)).unwrap(),
            None,
        )),
    ];
    for event in events {
        let new = SegmentRecord::new(original.header().clone(), event).unwrap();
        let old = String::from_utf8(new.to_canonical_json())
            .unwrap()
            .replace("\"schema_version\":4", "\"schema_version\":3")
            .replace(",\"venue_time\":null", "");
        for decoded in [
            SegmentRecord::from_json(old.as_bytes()).unwrap(),
            SegmentRecord::from_canonical_json(old.as_bytes()).unwrap(),
        ] {
            assert_eq!(decoded.schema_version(), 3);
            assert_eq!(decoded.header(), new.header());
            assert_eq!(decoded.event(), new.event());
            assert_eq!(decoded.to_canonical_json(), old.as_bytes());
        }
        let invalid_old = String::from_utf8(new.to_canonical_json())
            .unwrap()
            .replace("\"schema_version\":4", "\"schema_version\":3");
        assert!(SegmentRecord::from_json(invalid_old.as_bytes()).is_err());
        assert!(SegmentRecord::from_canonical_json(invalid_old.as_bytes()).is_err());
        let missing_new = String::from_utf8(new.to_canonical_json())
            .unwrap()
            .replace(",\"venue_time\":null", "");
        assert!(SegmentRecord::from_json(missing_new.as_bytes()).is_err());
    }
}

#[test]
fn venue_time_roundtrips_and_rejects_invalid_annotations_in_both_readers() {
    use replay_domain::{EventKind, Resolution, VenueTime};
    let SegmentEvent::Book(BookEvent::Delta(delta)) = record().event().clone() else {
        panic!()
    };
    let time = VenueTime::new(
        Some(1669149841123456000),
        Some(Resolution::Microsecond),
        Some(EventKind::ExchangeEvent),
        Some(1669149841130000000),
    )
    .unwrap();
    let annotated = SegmentRecord::new(
        record().header().clone(),
        SegmentEvent::Book(BookEvent::Delta(delta.with_venue_time(Some(time)))),
    )
    .unwrap();
    let canonical = String::from_utf8(annotated.to_canonical_json()).unwrap();
    assert!(canonical.contains("\"event_ns\":1669149841123456000,\"event_resolution\":\"microsecond\",\"event_kind\":\"exchange_event\",\"sent_ns\":1669149841130000000"));
    assert_eq!(
        SegmentRecord::from_canonical_json(canonical.as_bytes()).unwrap(),
        annotated
    );
    assert_eq!(
        SegmentRecord::from_json(canonical.as_bytes()).unwrap(),
        annotated
    );
    let plain = String::from_utf8(record().to_canonical_json()).unwrap();
    for invalid in [
        r#"{"event_ns":null,"event_resolution":null,"event_kind":null,"sent_ns":null}"#,
        r#"{"event_ns":null,"event_resolution":"millisecond","event_kind":null,"sent_ns":1}"#,
        r#"{"event_ns":1,"event_resolution":null,"event_kind":"book_update","sent_ns":null}"#,
        r#"{"event_ns":1,"event_resolution":"millisecond","event_kind":null,"sent_ns":null}"#,
        r#"{"event_ns":1,"event_resolution":"nanosecond","event_kind":"book_update","sent_ns":null}"#,
        r#"{"event_ns":1,"event_resolution":"millisecond","event_kind":"match","sent_ns":null}"#,
        r#"{"event_ns":1,"event_resolution":"millisecond","event_kind":"book_update","sent_ns":null,"unknown":1}"#,
        r#"{"event_ns":1,"event_resolution":"millisecond","event_kind":"book_update"}"#,
        r#"{"event_ns":1,"event_ns":2,"event_resolution":"millisecond","event_kind":"book_update","sent_ns":null}"#,
        r#"{"event_ns":18446744073709551616,"event_resolution":"millisecond","event_kind":"book_update","sent_ns":null}"#,
        r#"{"event_ns":"1","event_resolution":"millisecond","event_kind":"book_update","sent_ns":null}"#,
    ] {
        let invalid = plain.replace("\"venue_time\":null", &format!("\"venue_time\":{invalid}"));
        assert!(
            SegmentRecord::from_json(invalid.as_bytes()).is_err(),
            "{invalid}"
        );
        assert!(
            SegmentRecord::from_canonical_json(invalid.as_bytes()).is_err(),
            "{invalid}"
        );
    }
    let sent_only = VenueTime::new(None, None, None, Some(7)).unwrap();
    assert_eq!(
        serde_json::from_str::<VenueTime>(&serde_json::to_string(&sent_only).unwrap()).unwrap(),
        sent_only
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
    assert_eq!(
        header.provenance().source_segment_sha256().as_hex(),
        DIGEST_A
    );
    assert_eq!(header.provenance().source_line_number(), 17);
    assert_eq!(header.provenance().content_hash().as_hex(), DIGEST_B);
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
            "{\"schema_version\":4",
            "{\"unknown\":0,\"schema_version\":4",
            1,
        ),
        canonical.replacen("\"side\":\"bid\"", "\"side\":\"offer\"", 1),
        canonical.replacen("\"kind\":\"delta\"", "\"kind\":\"replace\"", 1),
        canonical.replacen("\"kind\":\"decrease\"", "\"kind\":\"shift\"", 1),
        canonical.replacen(
            "\"kind\":\"decrease\",\"value\"",
            "\"kind\":\"decrease\",\"unknown\":0,\"value\"",
            1,
        ),
        canonical.replacen("\"atoms\":5100", "\"atoms\":5100,\"float\":0.51", 1),
    ];
    for invalid in cases {
        assert!(SegmentRecord::from_canonical_json(invalid.as_bytes()).is_err());
    }
    let future = canonical.replacen("\"schema_version\":4", "\"schema_version\":5", 1);
    assert_eq!(
        SegmentRecord::from_canonical_json(future.as_bytes()),
        Err(DomainError::UnsupportedSchemaVersion(5))
    );
}

#[test]
fn state_hashes_are_algorithm_typed_and_strict() {
    let digest = "0123456789abcdef0123456789abcdef01234567";
    let hash = Sha1::from_hex(digest).unwrap();
    assert_eq!(hash.as_hex(), digest);
    assert_eq!(
        serde_json::to_string(&BookStateHash::Sha1(hash)).unwrap(),
        format!(r#"{{"algorithm":"sha1","digest":"{digest}"}}"#)
    );
    assert!(Sha1::from_hex(&"a".repeat(39)).is_err());
    assert!(Sha1::from_hex(&"A".repeat(40)).is_err());
    assert!(Sha1::from_hex(&"g".repeat(40)).is_err());
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
    let zero_change = canonical.replacen("\"atoms\":250", "\"atoms\":0", 1);
    assert!(SegmentRecord::from_canonical_json(zero_change.as_bytes()).is_err());
    let negative_quantity = canonical.replacen("\"atoms\":250", "\"atoms\":-1", 1);
    assert!(SegmentRecord::from_canonical_json(negative_quantity.as_bytes()).is_err());
    let excessive_quantity =
        canonical.replacen("\"atoms\":250", "\"atoms\":9223372036854775808", 1);
    assert!(SegmentRecord::from_canonical_json(excessive_quantity.as_bytes()).is_err());
}

#[test]
fn invariant_bearing_composites_validate_when_deserialized_directly() {
    let canonical = String::from_utf8(record().to_canonical_json()).unwrap();
    let value: serde_json::Value = serde_json::from_str(&canonical).unwrap();
    let header = &value["header"];
    let address = &header["address"];
    let provenance = &header["provenance"];

    let mut invalid_address = address.clone();
    invalid_address["canonical_seq"] = 0.into();
    assert!(serde_json::from_value::<EventAddress>(invalid_address).is_err());

    let mut invalid_provenance = provenance.clone();
    invalid_provenance["source_line_number"] = 0.into();
    assert!(serde_json::from_value::<CanonicalProvenance>(invalid_provenance).is_err());

    let mut invalid_header = header.clone();
    invalid_header["visible_ns"] = 1.into();
    assert!(serde_json::from_value::<EventHeader>(invalid_header).is_err());

    let px = |atoms| ConditionalMarketPrice::from_atoms(atoms, scale(2)).unwrap();
    let qty = |atoms| PositiveQty::new(Qty::from_atoms(atoms, scale(0)).unwrap()).unwrap();
    let full = FullBook::new(
        InstrumentId::new("venue:asset").unwrap(),
        ContractOrientation::Outcome,
        vec![Level::new(px(50), qty(1)), Level::new(px(40), qty(1))],
        vec![],
        None,
        None,
    )
    .unwrap();
    let invalid_full =
        serde_json::to_string(&full)
            .unwrap()
            .replacen("\"atoms\":50", "\"atoms\":40", 1);
    assert!(serde_json::from_str::<FullBook>(&invalid_full).is_err());
    let mut unsorted_full = serde_json::to_value(&full).unwrap();
    unsorted_full["bids"].as_array_mut().unwrap().reverse();
    assert!(serde_json::from_value::<FullBook>(unsorted_full).is_err());
    let mut mixed_scale_full = serde_json::to_value(&full).unwrap();
    mixed_scale_full["bids"][1]["price"]["scale"] = 3.into();
    assert!(serde_json::from_value::<FullBook>(mixed_scale_full).is_err());

    let anchor = AuditAnchor::new(
        InstrumentId::new("venue:asset").unwrap(),
        ContractOrientation::Outcome,
        vec![Level::new(px(50), qty(1)), Level::new(px(40), qty(1))],
        vec![],
        BookStateHash::Sha1(Sha1::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap()),
        None,
    )
    .unwrap();
    let invalid_anchor =
        serde_json::to_string(&anchor)
            .unwrap()
            .replacen("\"atoms\":50", "\"atoms\":40", 1);
    assert!(serde_json::from_str::<AuditAnchor>(&invalid_anchor).is_err());

    assert!(
        serde_json::from_str::<FaultImpact>(r#"{"kind":"requested_venue_books","value":""}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<NormalizationFault>(
            r#"{"reject_id":"","impact":{"kind":"unattributed_lane","value":"lane-a"}}"#
        )
        .is_err()
    );
}

#[test]
fn unsupported_segment_version_precedes_nested_invariant_failure() {
    let invalid = String::from_utf8(record().to_canonical_json())
        .unwrap()
        .replacen("\"schema_version\":4", "\"schema_version\":5", 1)
        .replacen("\"canonical_seq\":42", "\"canonical_seq\":0", 1);
    assert_eq!(
        SegmentRecord::from_canonical_json(invalid.as_bytes()),
        Err(DomainError::UnsupportedSchemaVersion(5))
    );
}

#[test]
fn direct_record_decode_preserves_duplicate_field_rejection() {
    let canonical = String::from_utf8(record().to_canonical_json()).unwrap();
    assert_eq!(
        serde_json::from_str::<SegmentRecord>(&canonical).unwrap(),
        record()
    );
    assert_eq!(
        serde_json::from_value::<SegmentRecord>(serde_json::to_value(record()).unwrap()).unwrap(),
        record()
    );
    let duplicate = canonical.replacen(
        "\"canonical_seq\":42",
        "\"canonical_seq\":42,\"canonical_seq\":42",
        1,
    );
    assert!(serde_json::from_str::<SegmentRecord>(&duplicate).is_err());
}

#[test]
fn single_pass_decode_equals_the_strict_decode() {
    let canonical = record().to_canonical_json();
    assert_eq!(SegmentRecord::from_json(&canonical).unwrap(), record());
    assert_eq!(
        SegmentRecord::from_json(&canonical),
        SegmentRecord::from_canonical_json(&canonical)
    );
    // The single pass proves structure and domain validity, not the encoding:
    // a pin's SHA-256 binding does that. Insignificant whitespace decodes to
    // the same record the strict path rejects as non-canonical.
    let with_newline = [canonical.clone(), b"\n".to_vec()].concat();
    assert_eq!(SegmentRecord::from_json(&with_newline).unwrap(), record());
    assert_eq!(
        SegmentRecord::from_canonical_json(&with_newline),
        Err(DomainError::NonCanonicalEncoding)
    );
}

#[test]
fn single_pass_decode_rejects_malformed_unknown_and_invalid_records() {
    let canonical = String::from_utf8(record().to_canonical_json()).unwrap();
    let header_start = canonical.find("\"header\"").unwrap();
    let event_start = canonical.find(",\"event\"").unwrap();
    let header_field = &canonical[header_start..event_start];
    let cases = [
        // malformed JSON and trailing data
        canonical[..canonical.len() - 1].to_owned(),
        format!("{canonical}x"),
        format!("{canonical}{canonical}"),
        "[3]".to_owned(),
        String::new(),
        // unknown, reordered, duplicate and missing top-level fields
        canonical.replacen(
            "{\"schema_version\":4",
            "{\"unknown\":0,\"schema_version\":4",
            1,
        ),
        canonical.replacen("}}}}", "}}},\"unknown\":0}", 1),
        canonical.replacen(
            &format!("\"schema_version\":4,{header_field}"),
            &format!("{header_field},\"schema_version\":4"),
            1,
        ),
        canonical.replacen("}}}}", &(String::from("}}},") + header_field + "}"), 1),
        canonical[..event_start].to_owned() + "}",
        // unknown nested fields and variants, duplicate nested field
        canonical.replacen("\"side\":\"bid\"", "\"side\":\"offer\"", 1),
        canonical.replacen("\"kind\":\"delta\"", "\"kind\":\"replace\"", 1),
        canonical.replacen("\"kind\":\"decrease\"", "\"kind\":\"shift\"", 1),
        canonical.replacen("\"atoms\":5100", "\"atoms\":5100,\"float\":0.51", 1),
        canonical.replacen(
            "\"continuity\":\"continuous\"",
            "\"continuity\":\"guessed\"",
            1,
        ),
        canonical.replacen(
            "\"canonical_seq\":42",
            "\"canonical_seq\":42,\"canonical_seq\":42",
            1,
        ),
        // invalid domain states
        canonical.replacen("\"atoms\":5100", "\"atoms\":-1", 1),
        canonical.replacen("\"atoms\":250", "\"atoms\":0", 1),
        canonical.replacen("\"atoms\":250", "\"atoms\":9223372036854775808", 1),
        canonical.replacen("\"canonical_seq\":42", "\"canonical_seq\":0", 1),
        canonical.replacen("\"visible_ns\":1785409600000000000", "\"visible_ns\":1", 1),
    ];
    for invalid in cases {
        assert_ne!(invalid, canonical);
        assert!(
            SegmentRecord::from_json(invalid.as_bytes()).is_err(),
            "accepted {invalid}"
        );
        assert!(
            SegmentRecord::from_canonical_json(invalid.as_bytes()).is_err(),
            "strict accepted {invalid}"
        );
    }
    // The version is checked before the header or event is interpreted.
    let future = canonical
        .replacen("\"schema_version\":4", "\"schema_version\":5", 1)
        .replacen("\"canonical_seq\":42", "\"canonical_seq\":0", 1);
    assert_eq!(
        SegmentRecord::from_json(future.as_bytes()),
        Err(DomainError::UnsupportedSchemaVersion(5))
    );
    let future = canonical.replacen("\"schema_version\":4", "\"schema_version\":99", 1);
    assert_eq!(
        SegmentRecord::from_json(future.as_bytes()),
        Err(DomainError::UnsupportedSchemaVersion(99))
    );
}

#[test]
fn side_orientation_and_absolute_relative_semantics_stay_distinct() {
    let decoded = SegmentRecord::from_canonical_json(&record().to_canonical_json()).unwrap();
    let SegmentEvent::Book(BookEvent::Delta(delta)) = decoded.event() else {
        panic!("expected delta")
    };
    assert_eq!(delta.side(), Side::Bid);
    assert_eq!(delta.orientation(), ContractOrientation::Complement);
    assert_eq!(
        delta.change(),
        LevelChange::Decrease(PositiveQty::parse("2.50", scale(2)).unwrap())
    );
}

#[test]
fn level_change_golden_vectors_round_trip() {
    let quantity = PositiveQty::parse("0.25", scale(2)).unwrap();
    let vectors = [
        (
            LevelChange::Set(quantity),
            r#"{"kind":"set","value":{"atoms":25,"scale":2,"unit":"contracts"}}"#,
        ),
        (LevelChange::Delete, r#"{"kind":"delete"}"#),
        (
            LevelChange::Increase(quantity),
            r#"{"kind":"increase","value":{"atoms":25,"scale":2,"unit":"contracts"}}"#,
        ),
        (
            LevelChange::Decrease(quantity),
            r#"{"kind":"decrease","value":{"atoms":25,"scale":2,"unit":"contracts"}}"#,
        ),
    ];
    for (value, expected) in vectors {
        assert_eq!(serde_json::to_string(&value).unwrap(), expected);
        assert_eq!(
            serde_json::from_str::<LevelChange>(expected).unwrap(),
            value
        );
    }
    assert!(
        serde_json::from_str::<LevelChange>(
            r#"{"kind":"increase","value":{"atoms":0,"scale":2,"unit":"contracts"}}"#,
        )
        .is_err()
    );
    assert!(serde_json::from_str::<LevelChange>(r#"{"kind":"future"}"#).is_err());
}

#[test]
fn full_books_sort_sides_and_reject_duplicate_or_nonpositive_levels() {
    let px = |atoms| ConditionalMarketPrice::from_atoms(atoms, scale(2)).unwrap();
    let qty = |atoms| PositiveQty::new(Qty::from_atoms(atoms, scale(0)).unwrap()).unwrap();
    let book = FullBook::new(
        InstrumentId::new("venue:asset").unwrap(),
        ContractOrientation::Outcome,
        vec![Level::new(px(40), qty(2)), Level::new(px(50), qty(1))],
        vec![Level::new(px(70), qty(1)), Level::new(px(60), qty(2))],
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
    assert!(PositiveQty::new(Qty::from_atoms(0, scale(0)).unwrap()).is_err());
    assert!(
        FullBook::new(
            InstrumentId::new("venue:asset").unwrap(),
            ContractOrientation::Outcome,
            vec![Level::new(px(50), qty(1)), Level::new(px(50), qty(2))],
            vec![],
            None,
            None,
        )
        .is_err()
    );
}
