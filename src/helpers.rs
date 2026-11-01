//! Human-readable rendering of extended-query protocol values for logging.
use bytes::Bytes;
use pgwire::api::Type;
use postgres_types::FromSql;

/// Render bound Bind-message parameters as a comma-separated string for log
/// output, e.g. `42, "abc", NULL`.
///
/// `params` holds the raw wire bytes of each parameter and `types` the OID
/// the parameter was described as; they are zipped positionally, so a
/// parameter with no matching type entry is dropped rather than reported.
///
/// A parameter is decoded through its `PostgreSQL` type only for the scalar
/// types listed here. Anything else, and anything that fails to decode (a
/// client that sent text format where binary was assumed, or a truncated
/// value), falls back to a `0x` hex dump of the raw bytes. Decoding is
/// deliberately lenient because this string only ever reaches a debug log -
/// it must never fail or panic and take a query down with it.
pub fn _debug_parameters(params: &[Option<Bytes>], types: &[Type]) -> String {
    params
        .iter()
        .zip(types.iter())
        .map(|(param, ty)| match param {
            None => "NULL".to_string(),
            Some(bytes) => {
                let buf = &bytes[..];
                let decoded = match ty {
                    &Type::INT2 => i16::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::INT4 => i32::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::INT8 => i64::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::FLOAT4 => f32::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::FLOAT8 => f64::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
                        String::from_sql(ty, buf).map(|s| format!("{s:?}"))
                    }
                    &Type::BOOL => bool::from_sql(ty, buf).map(|v| v.to_string()),
                    &Type::TIMESTAMP => {
                        chrono::NaiveDateTime::from_sql(ty, buf).map(|v| v.to_string())
                    }
                    &Type::TIMESTAMPTZ => {
                        chrono::DateTime::<chrono::Utc>::from_sql(ty, buf).map(|v| v.to_string())
                    }
                    _ => Err("unsupported type".into()),
                };
                decoded.unwrap_or_else(|_| format!("0x{}", hex::encode(bytes)))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
