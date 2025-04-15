use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::catalog::{TableProvider, SchemaProvider, MemorySchemaProvider};
use datafusion::logical_expr::{LogicalPlan, TableScan, Volatility};


use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{create_udf, ColumnarValue};

pub fn register_version_udf(ctx: &SessionContext) {
    let version_fn = |_: &[ColumnarValue]| {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("PostgreSQL 14.0 mock".to_string()))))
    };

    let udf = create_udf(
        "version",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(version_fn),
    );

    ctx.register_udf(udf);
}

pub fn register_info_schema_tables(ctx: &SessionContext) -> datafusion::error::Result<()> {
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("missing 'datafusion' catalog".to_string()))?;

    let schemas = ["information_schema", "pg_catalog"];
    for schema in schemas {
        if catalog.schema(schema).is_none() {
            let schema_provider = Arc::new(MemorySchemaProvider::new());
            catalog.register_schema(schema, schema_provider)?;
        }
    }

    let schema = catalog
        .schema("information_schema")
        .ok_or_else(|| DataFusionError::Plan("missing 'information_schema' schema".to_string()))?;


    let schema_tables = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
    ]));

    let batch_tables = RecordBatch::try_new(
        schema_tables.clone(),
        vec![
            Arc::new(StringArray::from(vec!["users", "items"])) as ArrayRef,
        ],
    )?;

    let table_tables = MemTable::try_new(schema_tables, vec![vec![batch_tables]])?;
    schema.register_table("tables".to_string(), Arc::new(table_tables))?;

    let schema_columns = Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
    ]));

    let batch_columns = RecordBatch::try_new(
        schema_columns.clone(),
        vec![
            Arc::new(StringArray::from(vec!["users", "users", "items", "items"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["id", "name", "id", "value"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["int8", "varchar", "int8", "varchar"])) as ArrayRef,
        ],
    )?;

    let table_columns = MemTable::try_new(schema_columns, vec![vec![batch_columns]])?;
    schema.register_table("columns".to_string(), Arc::new(table_columns))?;

    let pg_catalog = catalog
        .schema("pg_catalog")
        .ok_or_else(|| DataFusionError::Plan("missing 'pg_catalog' schema".to_string()))?;

    let schema_pg_tables = Arc::new(Schema::new(vec![
        Field::new("schemaname", DataType::Utf8, false),
        Field::new("tablename", DataType::Utf8, false),
        Field::new("tableowner", DataType::Utf8, false),
        Field::new("tablespace", DataType::Utf8, false),
        Field::new("hasindexes", DataType::Boolean, false),
        Field::new("hasrules", DataType::Boolean, false),
        Field::new("hastriggers", DataType::Boolean, false),
        Field::new("rowsecurity", DataType::Boolean, false),
    ]));

    let batch_pg_tables = RecordBatch::try_new(
        schema_pg_tables.clone(),
        vec![
            Arc::new(StringArray::from(vec!["public", "public"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["users", "items"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["owner", "owner"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["", ""])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
        ],
    )?;

    let table_pg_tables = MemTable::try_new(schema_pg_tables, vec![vec![batch_pg_tables]])?;
    pg_catalog.register_table("pg_tables".to_string(), Arc::new(table_pg_tables))?;

    Ok(())
}

pub async fn get_logical_plan(ctx: &SessionContext, query: &str) -> Option<LogicalPlan> {
    match ctx.sql(query).await {
        Ok(df) => {
            let plan = df.logical_plan().clone(); // Clone the owned plan
            println!("{:#?}", plan);
            Some(plan)
        }
        Err(e) => {
            eprintln!("Failed to create logical plan: {}", e);
            None
        }
    }
}

pub fn extract_schema_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(TableScan { table_name, .. }) => {
            match table_name {
                datafusion::catalog::TableReference::Bare { .. } => None,
                datafusion::catalog::TableReference::Partial { schema, .. } => Some(schema.to_string()),
                datafusion::catalog::TableReference::Full { schema, .. } => Some(schema.to_string()),
            }
        }
        _ => {

            for expr in plan.expressions() {
                if let Some(schema) = extract_schema_from_expr(&expr) {
                    return Some(schema);
                }
            }

            for input in plan.inputs() {
                if let Some(schema) = extract_schema_name(input) {
                    return Some(schema);
                }
            }
            None
        }
    }
}

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

fn extract_schema_from_expr(expr: &Expr) -> Option<String> {
    let mut schema_name = None;

    let _ = expr.apply(|e| {
        if let Expr::ScalarFunction(func) = e {
            if let Some((schema, _func)) = func.name().split_once('.') {
                schema_name = Some(schema.to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });

    schema_name
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_logical_plan_dump() {
        let ctx = SessionContext::new();
        register_info_schema_tables(&ctx).unwrap();

        let version_fn = |_: &[ColumnarValue]| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("PostgreSQL 14.0 mock".to_string()))))
        };

        let udf = create_udf(
            "pg_catalog.version",
            vec![],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(version_fn),
        );
        ctx.register_udf(udf);

        let plan = get_logical_plan(&ctx, "SELECT * FROM information_schema.columns").await;
        assert!(plan.is_some(), "Expected a logical plan, but got None");
        let schema_name = extract_schema_name(&plan.unwrap()).unwrap();
        assert_eq!(schema_name, "information_schema");
        println!("Schema name: {}", schema_name);

        let plan = get_logical_plan(&ctx, "SELECT * FROM pg_catalog.pg_tables").await;
        assert!(plan.is_some(), "Expected a logical plan, but got None");
        let schema_name = extract_schema_name(&plan.unwrap()).unwrap();
        assert_eq!(schema_name, "pg_catalog");
        println!("Schema name: {}", schema_name);

        let plan = get_logical_plan(&ctx, "SELECT pg_catalog.version()").await;
        assert!(plan.is_some(), "Expected a logical plan, but got None");
        let schema_name = extract_schema_name(&plan.unwrap()).unwrap();
        assert_eq!(schema_name, "pg_catalog");
        println!("Schema name: {}", schema_name);


        // let plan = get_logical_plan(&ctx, "SELECT * FROM users").await;
        // assert!(plan.is_none(), "Expected None");
    }
}