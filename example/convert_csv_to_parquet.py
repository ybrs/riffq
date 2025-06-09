import duckdb

duckdb.sql("""
COPY (
  SELECT * FROM read_csv_auto('data/*.csv')
) TO 'data/klines.parquet' (FORMAT 'parquet');
""")