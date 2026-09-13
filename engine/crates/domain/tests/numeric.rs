use replay_domain::{
    ConditionalMarketPrice, DecimalScale, Magnitude, NumericError, PositiveQty, PriceUnit, Px, Qty,
    QuantityUnit,
};

fn scale(value: u8) -> DecimalScale {
    DecimalScale::new(value).unwrap()
}

#[test]
fn unsigned_magnitudes_and_quantities_parse_exactly_without_floats() {
    let magnitude = Magnitude::parse("12.340", scale(2)).unwrap();
    assert_eq!(magnitude.atoms(), 1_234);
    assert_eq!(magnitude.scale(), scale(2));

    let quantity = Qty::parse("12.340", scale(2)).unwrap();
    assert_eq!(quantity.atoms(), 1_234);
    assert_eq!(quantity.unit(), QuantityUnit::Contracts);
    assert_eq!(Qty::parse("1.2", scale(4)).unwrap().atoms(), 12_000);

    for signed in ["-1", "-0", "+1"] {
        assert!(Qty::parse(signed, scale(0)).is_err(), "accepted {signed:?}");
    }
    assert_eq!(
        Magnitude::parse("-1", scale(0)),
        Err(NumericError::InvalidSyntax)
    );
}

#[test]
fn decimal_grammar_is_closed() {
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
}

#[test]
fn unicode_decimal_input_is_rejected_without_byte_boundary_panics() {
    let scale = scale(4);
    assert_eq!(Px::parse("0.000é", scale), Err(NumericError::InvalidSyntax));
    assert_eq!(Qty::parse("1.0é", scale), Err(NumericError::InvalidSyntax));
}

#[test]
fn quantity_logical_width_is_capped_below_its_u64_storage_width() {
    assert_eq!(
        Qty::parse("9223372036854775807", scale(0)).unwrap().atoms(),
        i64::MAX as u64
    );
    assert_eq!(
        Qty::parse("9223372036854775808", scale(0)),
        Err(NumericError::LogicalMaximumExceeded)
    );
    assert_eq!(
        Qty::from_atoms(i64::MAX as u64, scale(0)).unwrap().atoms(),
        i64::MAX as u64
    );
    assert_eq!(
        Qty::from_atoms(i64::MAX as u64 + 1, scale(0)),
        Err(NumericError::LogicalMaximumExceeded)
    );
    assert_eq!(DecimalScale::new(19), Err(NumericError::ScaleOutOfRange));
}

#[test]
fn positive_quantity_rejects_zero() {
    assert_eq!(
        PositiveQty::new(Qty::from_atoms(0, scale(2)).unwrap()),
        Err(NumericError::ZeroQuantity)
    );
    let quantity = PositiveQty::parse("0.01", scale(2)).unwrap();
    assert_eq!(quantity.atoms(), 1);
    assert_eq!(quantity.scale(), scale(2));
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
        Qty::from_atoms(i64::MAX as u64, scale(0))
            .unwrap()
            .checked_rescale(scale(1)),
        Err(NumericError::Overflow)
    );
    assert_eq!(
        Qty::from_atoms(1_000_000_000_000_000_000, scale(0))
            .unwrap()
            .checked_rescale(scale(1)),
        Err(NumericError::LogicalMaximumExceeded)
    );
}

#[test]
fn checked_quantity_addition_requires_matching_scale_and_checks_logical_maximum() {
    assert_eq!(
        Qty::from_atoms(10, scale(2))
            .unwrap()
            .checked_add(Qty::from_atoms(3, scale(2)).unwrap())
            .unwrap()
            .atoms(),
        13
    );
    assert_eq!(
        Qty::from_atoms(10, scale(2))
            .unwrap()
            .checked_add(Qty::from_atoms(10, scale(3)).unwrap()),
        Err(NumericError::InexactRescale)
    );
    assert_eq!(
        Qty::from_atoms(i64::MAX as u64, scale(0))
            .unwrap()
            .checked_add(Qty::from_atoms(1, scale(0)).unwrap()),
        Err(NumericError::LogicalMaximumExceeded)
    );
}

#[test]
fn conditional_market_price_enforces_inclusive_unit_interval_at_every_scale() {
    assert_eq!(Px::parse("12.34", scale(2)).unwrap().atoms(), 1_234);
    for value in [
        ConditionalMarketPrice::parse("0", scale(0)).unwrap(),
        ConditionalMarketPrice::parse("1", scale(0)).unwrap(),
        ConditionalMarketPrice::parse("0.0000", scale(4)).unwrap(),
        ConditionalMarketPrice::parse("1.0000", scale(4)).unwrap(),
    ] {
        let encoded = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::from_slice::<ConditionalMarketPrice>(&encoded).unwrap(),
            value
        );
        assert_eq!(value.unit(), PriceUnit::QuotePerContract);
    }
    assert_eq!(
        ConditionalMarketPrice::parse("1.0001", scale(4)),
        Err(NumericError::ConditionalPriceOutOfRange)
    );
    assert_eq!(
        ConditionalMarketPrice::parse("-0.0001", scale(4)),
        Err(NumericError::NegativePrice)
    );
    assert_eq!(
        ConditionalMarketPrice::try_from(Px::from_atoms(101, scale(2)).unwrap()),
        Err(NumericError::ConditionalPriceOutOfRange)
    );
    assert_eq!(
        ConditionalMarketPrice::parse("1.0000", scale(4))
            .unwrap()
            .checked_rescale(scale(2))
            .unwrap()
            .atoms(),
        100
    );
}
