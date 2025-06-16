use std::collections::HashMap;

/// Dispatch a query to the builtin pg_catalog emulation.
///
/// Returns `Some((schema, rows))` if the query is handled by the emulation,
/// otherwise `None` so the caller can fallback to Python handlers.
pub async fn dispatch(query: &str) -> Option<(Vec<HashMap<String, String>>, Vec<Vec<Option<String>>>)> {
    let q = query.trim().to_lowercase();
    if q == "select pg_catalog.version()" {
        let mut col = HashMap::new();
        col.insert("name".to_string(), "version".to_string());
        col.insert("type".to_string(), "string".to_string());
        let schema = vec![col];
        let rows = vec![vec![Some("PostgreSQL 14.13".to_string())]];
        return Some((schema, rows));
    }

    if q.contains("pg_catalog.pg_class") {
        let mut col = HashMap::new();
        col.insert("name".to_string(), "relname".to_string());
        col.insert("type".to_string(), "string".to_string());
        let schema = vec![col];
        let rows = vec![vec![Some("pg_class".to_string())]];
        return Some((schema, rows));
    }

    None
}
