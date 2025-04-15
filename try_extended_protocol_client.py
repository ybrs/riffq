import psycopg

with psycopg.connect("postgresql://user:password@localhost:5433/dbname") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT %s AS foo", (42,))
        print(cur.fetchone())
        cur.execute("SELECT %s AS foo", ('42',))
        print(cur.fetchone())


# from sqlalchemy import create_engine, text
#
#
# engine = create_engine("postgresql://myuser:mypassword@127.0.0.1:5433/mydb")
# conn = engine.connect()
# with engine.connect() as conn:
#     result = conn.execute(text("SELECT * FROM test;"))
#     rows = result.mappings().all()
#
# for row in rows:
#     print(dict(row))
#

# # Replace this with your actual server/port
# engine = create_engine("postgresql+psycopg2://user@localhost:5433/dbname")
# with engine.connect() as conn:
#     result = conn.execute(text("SELECT 1 + 2 AS bar"))
#     for row in result:
#         print(dict(row))
#
# with engine.connect() as conn:
#     stmt = text("SELECT :value AS foo")
#     result = conn.execute(stmt, {"value": 42})
#     for row in result:
#         print("row", row)
        # print(dict(row))
#
