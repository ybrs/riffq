"""Lazy (callback-driven) catalog example.

Riffq can answer PostgreSQL catalog queries (``pg_catalog`` / ``information_schema``)
from a *source object* that is consulted on every scan, instead of a snapshot
registered up front. This example backs that source with a plain in-memory dict
-- no external engine -- so you can see the whole contract in one file.

Run it::

    python example/lazy_catalog.py

then connect with psql and watch the catalog stay in sync as you create tables::

    psql -h 127.0.0.1 -p 5444 -U user -d appdb

    appdb=> SELECT relname FROM pg_class WHERE relname='orders';   -- 0 rows
    appdb=> CREATE TABLE orders(id INT, total INT);                -- handled below
    appdb=> SELECT relname FROM pg_class WHERE relname='orders';   -- now 1 row
    appdb=> \\d orders
"""

import logging

import pyarrow as pa
import riffq

logging.basicConfig(level=logging.INFO)

# The "engine": a live, in-memory catalog. In a real app this would be DuckDB,
# PostgreSQL, a config file, a remote service -- anything. The lazy source below
# reads whatever is here *at query time*, so mutating it is reflected instantly.
CATALOG = {
    "appdb": {
        "public": {
            # table_name -> list of (column_name, pg_type_oid, nullable)
            "users": [("id", 23, False), ("name", 25, True)],  # 23=int4, 25=text
        }
    }
}

# pg_type OIDs the example understands, keyed by a coarse type name.
TYPE_OIDS = {"int": 23, "bigint": 20, "text": 25, "bool": 16, "float": 701}


def stable_oid(salt: str, *parts: str) -> int:
    """Derive a stable, built-in-clear OID from a name.

    The same inputs always return the same OID, so ``pg_class.oid`` and
    ``pg_attribute.attrelid`` agree across scans and catalog joins resolve.
    Distinct object classes use distinct salts to avoid collisions, and the
    result is kept well above the built-in OID range.
    """
    h = 5381
    for ch in (salt + "\x00" + "\x00".join(parts)):
        h = (h * 33 + ord(ch)) & 0x7FFFFFFF
    return 16384 + (h % 2_000_000_000)


class DictCatalogSource:
    """A lazy catalog source over the in-memory ``CATALOG`` dict.

    Each method receives a ``callback`` and invokes it with a list of row dicts,
    mirroring Riffq's Rust ``LazyCatalogSource`` trait one method per level.
    """

    def databases(self, callback):
        callback(
            [{"oid": stable_oid("db", name), "name": name} for name in CATALOG]
        )

    def schemas(self, database, callback):
        callback(
            [
                {"oid": stable_oid("ns", database, schema), "name": schema}
                for schema in CATALOG.get(database, {})
            ]
        )

    def relations(self, database, schema, callback):
        tables = CATALOG.get(database, {}).get(schema, {})
        callback(
            [
                {
                    "oid": stable_oid("rel", database, schema, name),
                    "reltype_oid": stable_oid("type", database, schema, name),
                    "name": name,
                    "kind": "table",
                }
                for name in tables
            ]
        )

    def columns(self, database, schema, relation, callback):
        cols = CATALOG.get(database, {}).get(schema, {}).get(relation, [])
        callback(
            [
                {"name": col, "type_oid": type_oid, "nullable": nullable}
                for (col, type_oid, nullable) in cols
            ]
        )


class Connection(riffq.BaseConnection):
    """Data path. Catalog queries are served by the lazy source above; everything
    else lands here. We implement a toy ``CREATE TABLE`` so you can watch the
    catalog update live, and echo a single row for any other query."""

    def handle_auth(self, user, password, host, database=None, callback=callable):
        # Accept any credentials in this example.
        return callback(True)

    def handle_query(self, sql, callback=callable, **kwargs):
        text = sql.strip().rstrip(";")
        low = text.lower()

        if low.startswith("create table "):
            # "create table public.orders(id int, total int)" (schema optional)
            head, _, body = text[len("create table "):].partition("(")
            qualified = head.strip()
            schema, _, name = qualified.rpartition(".")
            schema = schema or "public"
            cols = []
            for part in body.rstrip(")").split(","):
                bits = part.split()
                if len(bits) >= 2:
                    cols.append((bits[0].strip('"'), TYPE_OIDS.get(bits[1].lower(), 25), True))
            CATALOG.setdefault("appdb", {}).setdefault(schema, {})[name] = cols
            return callback("CREATE TABLE", is_tag=True)

        # Any other statement: return a single dummy row so clients are happy.
        batch = self.arrow_batch([pa.array([1])], ["?column?"])
        return self.send_reader(batch, callback)


def main():
    server = riffq.RiffqServer("127.0.0.1:5444", connection_cls=Connection)
    # Install the lazy source instead of register_database/schema/table.
    server.set_lazy_catalog(DictCatalogSource())
    server.start(catalog_emulation=True)


if __name__ == "__main__":
    main()
