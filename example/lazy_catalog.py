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

import itertools
import logging
import threading

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

# The example's own identifier registry: the first sight of an object assigns
# the next number and RECORDS it, so every later scan answers the same -- the
# same get-or-allocate rule riffq's persistent oid registry applies to declared
# objects. A source must own its identifiers (riffq passes them through
# verbatim and ships no derivation), and owning state makes a counter the right
# tool: 16384 is where PostgreSQL's reserved built-in range ends. The lock
# matters even here -- scans from concurrent connections race, and an unlocked
# check-then-act can hand one object two numbers.
OID_REGISTRY = {}
_oid_counter = itertools.count(16384)
_oid_lock = threading.Lock()


def oid_for(kind, *names):
    """The recorded oid for the object `names` describes, allocating on first
    sight. `kind` ("db", "ns", "rel", "type") keeps a database and a table
    that share a name apart."""
    key = (kind, *names)
    with _oid_lock:
        if key not in OID_REGISTRY:
            OID_REGISTRY[key] = next(_oid_counter)
        return OID_REGISTRY[key]


class DictCatalogSource:
    """A lazy catalog source over the in-memory ``CATALOG`` dict.

    Each method receives a ``callback`` and invokes it with a list of row dicts,
    mirroring Riffq's Rust ``LazyCatalogSource`` trait one method per level.
    """

    def databases(self, callback):
        callback(
            [{"oid": oid_for("db", name), "name": name} for name in CATALOG]
        )

    def schemas(self, database, callback):
        callback(
            [
                {"oid": oid_for("ns", database, schema), "name": schema}
                for schema in CATALOG.get(database, {})
            ]
        )

    def relations(self, database, schema, callback):
        tables = CATALOG.get(database, {}).get(schema, {})
        callback(
            [
                {
                    "oid": oid_for("rel", database, schema, name),
                    "reltype_oid": oid_for("type", database, schema, name),
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

    # Both methods below are OPTIONAL. Omit them and pg_config / pg_settings
    # serve their built-in defaults. Implement them to override or extend those
    # rows; a row whose ``name`` matches a built-in one replaces it.

    def config(self, callback):
        """``pg_catalog.pg_config`` build settings - report our own version."""
        callback([{"name": "VERSION", "setting": "riffq lazy-catalog example"}])

    def settings(self, callback):
        """``pg_catalog.pg_settings`` runtime parameters - override the live
        value of session-mutable ones (here, ``search_path``)."""
        callback([{"name": "search_path", "setting": "public"}])


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
