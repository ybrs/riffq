//! Arrow → pgwire type mapping.
//!
//! Keep this in one place so the encoder, DESCRIBE-only execution
//! and any future planner code all agree on column OIDs.
use log::info;

use arrow::datatypes::DataType;
use pgwire::api::Type;
// use arrow_schema::extension::CanonicalExtensionType;

// pub fn arrow_field_to_pgwire(field: &Field) -> Type {
//     if matches!(field.data_type(), DataType::FixedSizeBinary(16))
//         && field.extension_type_name() == Some(arrow_uuid::Uuid::NAME)
//     {
//         Type::UUID
//     } else {
//         arrow_type_to_pgwire(field.data_type())
//     }
// }


/// Translate an Arrow `DataType` into a pgwire `Type` (PostgreSQL OID).
///
/// Only scalar / first-class SQL-ish types have direct mappings.
/// Complex or nested types are passed through as TEXT so that the
/// frontend driver at least receives something printable.
///
/// If you later add support for PostgreSQL domains or custom OIDs,
/// extend this function – **never** add ad-hoc matches elsewhere.
pub fn arrow_type_to_pgwire(dt: &DataType) -> Type {
    use DataType::*;
    info!("Datatype -> {:?}", dt);
    match dt {
        /* ── integers ───────────────────────────── */
        Int8 | UInt8                   => Type::INT2,   // SMALLINT
        Int16                          => Type::INT2,
        UInt16                         => Type::INT4,
        Int32                          => Type::INT4,
        UInt32 | Int64 | UInt64        => Type::INT8,   // BIGINT

        /* ── floats & decimals ──────────────────── */
        Float16 | Float32              => Type::FLOAT4,
        Float64                        => Type::FLOAT8,
        Decimal128(_, _) | Decimal256(_, _)
                                        => Type::NUMERIC,

        /* ── booleans / strings / bytes ─────────── */
        Boolean                        => Type::BOOL,
        Utf8 | LargeUtf8               => Type::VARCHAR,
        Binary | LargeBinary | FixedSizeBinary(_)
                                        => Type::BYTEA,

        /* ── temporal ───────────────────────────── */
        Date32 | Date64                => Type::DATE,
        Time32(_) | Time64(_)          => Type::TIME,
        Timestamp(_, tz)               =>
            if tz.is_some() { Type::TIMESTAMPTZ } else { Type::TIMESTAMP },
        Duration(_) | Interval(_)      => Type::INTERVAL,
        /* ── UUID ──────────────────────────────── */        
        // TODO: we probably need to support uuid with something similar to this 
        //pub fn arrow_field_to_pgwire(field: &Field) -> Type {
        //     use DataType::*;

        //     // Look for the extension marker first
        //     if let Some(ext) = field.metadata().get("ARROW:extension:name") {
        //         if ext == "uuid" {
        //             return Type::UUID;
        //         }
        //     }

        //     match field.data_type() {
        //         FixedSizeBinary(16)           => Type::BYTEA,   // raw binary, *not* UUID
        //         // … all the other matches exactly like before …
        //         _                              => Type::VARCHAR,
        //     }
        // }

        // /* ── everything else: send as text ─────── */
        // List(_)
        // | LargeList(_)
        // | FixedSizeList(_, _)
        // | Struct(_)
        // | Map(_, _)
        // | Union(_, _)              // two-field variant in Arrow 55
        // | Dictionary(_, _)
        // | RunEndEncoded(_, _)
        // | Null                      => Type::VARCHAR,
        
        /* ── arrays ─────────────────────────────── */
        List(field) | LargeList(field) | FixedSizeList(field, _) => match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR_ARRAY,
            _ => Type::VARCHAR,
        },

        /* ── everything complex / unsupported ─── */
        _ => Type::VARCHAR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{TimeUnit};

    #[test]
    fn test_arrow_type_to_pgwire_integers() {
        assert_eq!(arrow_type_to_pgwire(&DataType::Int8), Type::INT2);
        assert_eq!(arrow_type_to_pgwire(&DataType::Int16), Type::INT2);
        assert_eq!(arrow_type_to_pgwire(&DataType::Int32), Type::INT4);
        assert_eq!(arrow_type_to_pgwire(&DataType::Int64), Type::INT8);
        assert_eq!(arrow_type_to_pgwire(&DataType::UInt8), Type::INT2);
        assert_eq!(arrow_type_to_pgwire(&DataType::UInt16), Type::INT4);
        assert_eq!(arrow_type_to_pgwire(&DataType::UInt32), Type::INT8);
        assert_eq!(arrow_type_to_pgwire(&DataType::UInt64), Type::INT8);
    }

    #[test]
    fn test_arrow_type_to_pgwire_floats() {
        assert_eq!(arrow_type_to_pgwire(&DataType::Float16), Type::FLOAT4);
        assert_eq!(arrow_type_to_pgwire(&DataType::Float32), Type::FLOAT4);
        assert_eq!(arrow_type_to_pgwire(&DataType::Float64), Type::FLOAT8);
    }

    #[test]
    fn test_arrow_type_to_pgwire_misc() {
        assert_eq!(arrow_type_to_pgwire(&DataType::Boolean), Type::BOOL);
        assert_eq!(arrow_type_to_pgwire(&DataType::Utf8), Type::VARCHAR);
        assert_eq!(arrow_type_to_pgwire(&DataType::LargeUtf8), Type::VARCHAR);
        assert_eq!(arrow_type_to_pgwire(&DataType::Date32), Type::DATE);
        assert_eq!(arrow_type_to_pgwire(&DataType::Date64), Type::DATE);
        assert_eq!(arrow_type_to_pgwire(&DataType::Decimal128(16, 6)), Type::NUMERIC);
        assert_eq!(arrow_type_to_pgwire(&DataType::Decimal256(38, 10)), Type::NUMERIC);
        assert_eq!(
            arrow_type_to_pgwire(&DataType::Timestamp(TimeUnit::Second, None)),
            Type::TIMESTAMP
        );
        assert_eq!(
            arrow_type_to_pgwire(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            Type::TIMESTAMP
        );
        assert_eq!(
            arrow_type_to_pgwire(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Type::TIMESTAMP
        );
        assert_eq!(
            arrow_type_to_pgwire(&DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Type::TIMESTAMP
        );
    }
}
