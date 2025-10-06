"""
This is for simply printing out queries coming from postgresl client and sending it to 
postgresql server.
"""
import logging
import psycopg
import pyarrow as pa
import riffq
import argparse
import sqlparse
from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import Terminal256Formatter
import time
from psycopg import RawCursor


logging.basicConfig(level=logging.DEBUG)




class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        # simple username/password check
        callback(True)

    def handle_connect(self, ip, port, callback=callable):
        # allow every incoming connection
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        print(time.time(), "---" * 5)
        sql = sql.strip().lower().split(';')[0]
        if sql == "":
            print("empty query returning OK")
            return callback("OK", is_tag=True)

        try:
            formatted_sql = sqlparse.format(
                sql,
                reindent=True,
                keyword_case="upper",
                indent_width=4,
                strip_whitespace=False
            )

            lexer = get_lexer_by_name("postgresql")
            print(highlight(formatted_sql, lexer, Terminal256Formatter(style="monokai")), end="")

        except Exception as e:
            print(e)

        print("kw", kwargs)

        try:
            cursor = upstream_conn.cursor()

            if kwargs.get('query_args', None):
                cursor.execute(sql, kwargs['query_args'])
            else:
                cursor.execute(sql)

            # If no resultset (e.g., DML/DDL), return status tag
            if cursor.description is None:
                status = getattr(cursor, "statusmessage", "") or ""
                return callback(status, is_tag=True)

            # Build Arrow arrays column-wise to preserve types
            column_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            print("rows", rows)
            # Transpose rows into columns and let pyarrow infer types
            columns = list(zip(*rows)) if rows else [[] for _ in column_names]
            arrays = [pa.array(list(col)) for col in columns]

            # Log columns and their inferred Arrow types
            if column_names:
                cols_types = ", ".join(f"{n}: {a.type}" for n, a in zip(column_names, arrays))
                print(f"Columns: {cols_types}")

            reader = self.arrow_batch(values=arrays, names=column_names)
            return self.send_reader(reader, callback)
        except Exception as exc:
            # Forward upstream errors to the client
            sqlstate = getattr(exc, "pgcode", None) or "XX000"
            message = getattr(exc, "pgerror", None) or str(exc)
            print("-> error in query", message)
            try:
                # reset aborted transaction state so subsequent queries work
                if hasattr(upstream_conn, "rollback"):
                    upstream_conn.rollback()
            except Exception as rb_exc:
                logging.warning(f"failed to rollback upstream connection after error: {rb_exc}")
            return callback(("ERROR", sqlstate, message), is_error=True)
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def handle_query(self, sql, callback=callable, **kwargs):
        self._handle_query(sql, callback, **kwargs)
        # self.executor.submit(self._handle_query, sql, callback, **kwargs)

def main():

    parser = argparse.ArgumentParser(description="Show columns and types for a SQL query")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=False)
    parser.add_argument("--dbname", default="postgres")
    # TODO: add listen host and listen port
    args = parser.parse_args()

    global upstream_conn
    upstream_conn = psycopg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        cursor_factory=RawCursor
    )
    # server_version = upstream_conn.get_parameter_status("server_version")

    server = riffq.RiffqServer("0.0.0.0:5433", connection_cls=Connection)
    server.start(tls=False, catalog_emulation=False, server_version="17.02")

if __name__ == "__main__":
    main()
