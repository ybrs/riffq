use bytes::Bytes;
use postgres_types::FromSql;
use pgwire::api::{Type};

pub fn _debug_parameters(params: &[Option<Bytes>], types: &[Type]) -> String {
    params.iter().zip(types.iter()).map(|(param, ty)| {
        match param {
            None => "NULL".to_string(),
            Some(bytes) => {
                let mut buf = &bytes[..];
                let decoded = match ty {
                    &Type::INT2 => i16::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::INT4 => i32::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::INT8 => i64::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::FLOAT4 => f32::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::FLOAT8 => f64::from_sql(ty, &mut buf).map(|v| v.to_string()),
                    &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
                        String::from_sql(ty, &mut buf).map(|s| format!("{:?}", s))
                    }
                    _ => Err("unsupported type".into())
                };
                decoded.unwrap_or_else(|_| format!("0x{}", hex::encode(bytes)))
            }
        }
    }).collect::<Vec<_>>().join(", ")
}
