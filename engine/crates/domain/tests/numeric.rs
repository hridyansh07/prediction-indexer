use replay_domain::{DecimalScale, NumericError, PriceUnit, Px, Qty, QuantityUnit};

fn scale(value: u8) -> DecimalScale {
    DecimalScale::new(value).unwrap()
}

#[test]
fn decimals_parse_exactly_without_floats() {
    let price = Px::parse("0.5100", scale(4)).unwrap();
    assert_eq!(price.atoms(), 5_100);
    assert_eq!(price.scale(), scale(4));
    assert_eq!(price.unit(), PriceUnit::QuotePerContract);

    let quantity = Qty::parse("-12.340", scale(2)).unwrap();
    assert_eq!(quantity.atoms(), -1_234);
    assert_eq!(quantity.unit(), QuantityUnit::Contracts);
    assert_eq!(Qty::parse("1.2", scale(4)).unwrap().atoms(), 12_000);
}

#[test]
fn decimal_grammar_and_sign_are_closed() {
    for malformed in ["", "+1", " 1", "1 ", ".1", "1.", "1e2", "--1", "1.2.3"] {
        assert!(
            Px::parse(malformed, scale(2)).is_err(),
            "accepted {malformed:?}"
        );
    }
    assert_eq!(
        Px::parse("-0.1", scale(2)),
        Err(NumericError::NegativePrice)
    );
    assert_eq!(Qty::parse("-0.1", scale(2)).unwrap().atoms(), -10);
    assert_eq!(Qty::parse("-0", scale(0)).unwrap().atoms(), 0);
}

#[test]
fn decimal_width_boundaries_are_checked() {
    assert_eq!(
        Px::parse("9223372036854775807", scale(0)).unwrap().atoms(),
        i64::MAX
    );
    assert_eq!(
        Qty::parse("-9223372036854775808", scale(0))
            .unwrap()
            .atoms(),
        i64::MIN
    );
    assert_eq!(
        Px::parse("9223372036854775808", scale(0)),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        Qty::parse("-9223372036854775809", scale(0)),
        Err(NumericError::Overflow)
    );
    assert_eq!(DecimalScale::new(19), Err(NumericError::ScaleOutOfRange));
}

#[test]
fn excess_zero_precision_is_exact_but_nonzero_precision_is_rejected() {
    assert_eq!(Px::parse("0.510000", scale(4)).unwrap().atoms(), 5_100);
    assert_eq!(
        Px::parse("0.51001", scale(4)),
        Err(NumericError::InexactRescale)
    );
}

#[test]
fn rescale_distinguishes_underflow_inexactness_and_overflow() {
    assert_eq!(
        Px::parse("0.5100", scale(4))
            .unwrap()
            .checked_rescale(scale(2))
            .unwrap()
            .atoms(),
        51
    );
    assert_eq!(
        Qty::parse("0.001", scale(3))
            .unwrap()
            .checked_rescale(scale(2)),
        Err(NumericError::Underflow)
    );
    assert_eq!(
        Qty::parse("1.011", scale(3))
            .unwrap()
            .checked_rescale(scale(2)),
        Err(NumericError::InexactRescale)
    );
    assert_eq!(
        Qty::from_atoms(i64::MAX, scale(0)).checked_rescale(scale(1)),
        Err(NumericError::Overflow)
    );
}

#[test]
fn checked_quantity_addition_requires_matching_scale_and_checks_overflow() {
    assert_eq!(
        Qty::from_atoms(10, scale(2))
            .checked_add(Qty::from_atoms(-3, scale(2)))
            .unwrap()
            .atoms(),
        7
    );
    assert_eq!(
        Qty::from_atoms(10, scale(2)).checked_add(Qty::from_atoms(10, scale(3))),
        Err(NumericError::InexactRescale)
    );
    assert_eq!(
        Qty::from_atoms(i64::MAX, scale(0)).checked_add(Qty::from_atoms(1, scale(0))),
        Err(NumericError::Overflow)
    );
}
