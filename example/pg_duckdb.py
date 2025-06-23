import logging
import duckdb
import pyarrow as pa
import riffq
logging.basicConfig(level=logging.DEBUG)

class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        cur = duckdb_con.cursor()
        text = sql.strip().lower().split(';')[0]

        if text == "select pg_catalog.version()":
            batch = self.arrow_batch(
                [pa.array(["PostgreSQL 14.13"])],
                ["version"],
            )
            return self.send_reader(batch, callback)

        if text == "show transaction isolation level":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)

        if text == "show standard_conforming_strings":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)
        
        if text == "select current_schema()":
            batch = self.arrow_batch(
                [pa.array(["public"])],
                ["current_schema"],
            )
            return self.send_reader(batch, callback)

        try:
            reader = cur.execute(sql).fetch_record_batch()
            self.send_reader(reader, callback)
        except Exception as exc:
            logging.exception("error on executing query")
            batch = self.arrow_batch(
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

def main():
    global duckdb_con
    duckdb_con = duckdb.connect()
    duckdb_con.execute(
        """
        CREATE VIEW klines AS 
        SELECT * 
        FROM 'data/klines.parquet'
        """
    )
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.set_tls("certs/server.crt", "certs/server.key")
    server.start()

if __name__ == "__main__":
    main()
