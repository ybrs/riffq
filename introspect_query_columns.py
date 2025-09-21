#!/usr/bin/env python3
import argparse
import psycopg2

def main():
    parser = argparse.ArgumentParser(description="Show columns and types for a SQL query")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=False)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("query", help="SQL query to analyze")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )
    cur = conn.cursor()
    cur.execute(args.query)

    type_oids = tuple({col.type_code for col in cur.description})
    type_names = {}
    # if type_oids:
    #     typcur = conn.cursor()
    #     typcur.execute("SELECT oid, format_type(oid, NULL) FROM pg_type WHERE oid = ANY(%s)", (list(type_oids),))
    #     type_names = {oid: name for oid, name in typcur.fetchall()}
    #     typcur.close()

    for col in cur.description:
        tname = type_names.get(col.type_code, "")
        print(f"{col.name}\t{col.type_code}\t{tname}")

    print("\nData:")
    rows = cur.fetchmany(10000)
    for row in rows:
        print(row)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
