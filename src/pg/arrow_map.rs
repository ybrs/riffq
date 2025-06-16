use arrow::datatypes::DataType;
use pgwire::api::Type;

pub fn arrow_type_to_pgwire(dt: &DataType) -> Type {
    use DataType::*;
    match dt {
        Int8 | UInt8 => Type::INT2,
        Int16 => Type::INT2,
        UInt16 => Type::INT4,
        Int32 => Type::INT4,
        UInt32 | Int64 | UInt64 => Type::INT8,
        Float16 | Float32 => Type::FLOAT4,
        Float64 => Type::FLOAT8,
        Boolean => Type::BOOL,
        Utf8 | LargeUtf8 => Type::VARCHAR,
        Date32 | Date64 => Type::DATE,
        Timestamp(_, _) => Type::TIMESTAMP,
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
