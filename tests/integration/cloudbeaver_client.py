"""A minimal GraphQL client for driving CloudBeaver headlessly.

CloudBeaver's whole API is one GraphQL endpoint, and its session lives in a
cookie, so a client needs little more than a cookie jar and a JSON POST. Only
the standard library is used, keeping this tier from adding a test-time
dependency the other tiers do not have.

The navigator methods here walk the same node tree the CloudBeaver web UI
expands, which is what makes this tier meaningful: each expansion runs
DBeaver's own PostgreSQL catalog SQL against riffq.
"""
import http.cookiejar
import json
import urllib.request

# Node type ids DBeaver uses for the folders under a schema and a table. They
# appear verbatim in navigator node paths, so the tier addresses nodes by these
# rather than by their display names, which are localised.
SCHEMA_FOLDER = "org.jkiss.dbeaver.ext.postgresql.model.PostgreSchema"
DATABASE_FOLDER = "org.jkiss.dbeaver.ext.postgresql.model.PostgreDatabase"
TABLE_FOLDER = "org.jkiss.dbeaver.ext.postgresql.model.PostgreTable"
COLUMN_FOLDER = "org.jkiss.dbeaver.ext.postgresql.model.PostgreTableColumn"


class CloudBeaverError(Exception):
    """Raised when CloudBeaver answers a GraphQL request with errors."""


class CloudBeaverClient:
    """A session against one CloudBeaver server.

    Attributes:
        endpoint: The GraphQL endpoint URL.
    """

    def __init__(self, web_port, host="127.0.0.1", timeout=180):
        """Open a client against a CloudBeaver server.

        Args:
            web_port: The port CloudBeaver serves on.
            host: The host CloudBeaver serves on.
            timeout: Per-request timeout in seconds. Metadata calls hit a real
                database through a JDBC driver, so this is generous.
        """
        self.endpoint = f"http://{host}:{web_port}/api/gql"
        self._timeout = timeout
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
        )

    def execute(self, query, variables=None):
        """Run one GraphQL document and return its data.

        Args:
            query: The GraphQL query or mutation text.
            variables: Optional variables mapping.

        Returns:
            The "data" object from the response.

        Raises:
            CloudBeaverError: If the response carries GraphQL errors.
        """
        payload = json.dumps({"query": query, "variables": variables or {}})
        request = urllib.request.Request(
            self.endpoint,
            data=payload.encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with self._opener.open(request, timeout=self._timeout) as response:
            body = json.load(response)
        if "errors" in body:
            messages = "; ".join(error.get("message", "") for error in body["errors"])
            raise CloudBeaverError(messages)
        return body["data"]

    def open_session(self):
        """Start a session, which the cookie jar then carries on every call."""
        return self.execute("mutation { openSession { valid } }")

    def connect(self, connection_id):
        """Connect the pre-seeded data source, returning its connection info.

        Args:
            connection_id: The id from initial-data-sources.conf.

        Returns:
            The connection's info object.
        """
        data = self.execute(
            "mutation Init($id: ID!) { initConnection(id: $id) "
            "{ id name connected } }",
            {"id": connection_id},
        )
        return data["initConnection"]

    def children(self, node_path):
        """List a navigator node's children.

        Each call makes DBeaver run the catalog SQL for that node type, which
        is the point of this tier.

        Args:
            node_path: The navigator node path to expand.

        Returns:
            A list of child node objects with id, name, and nodeType.
        """
        data = self.execute(
            "query Nav($path: ID!) { navNodeChildren(parentPath: $path) "
            "{ id name nodeType } }",
            {"path": node_path},
        )
        return data["navNodeChildren"]

    def child_names(self, node_path):
        """Return just the names of a node's children, in the order given."""
        return [child["name"] for child in self.children(node_path)]

    def database_path(self, connection_id, database):
        """Build the navigator path of one database."""
        return f"database://{connection_id}/{DATABASE_FOLDER}/{database}"

    def schema_path(self, connection_id, database, schema):
        """Build the navigator path of one schema."""
        return (
            f"{self.database_path(connection_id, database)}/{SCHEMA_FOLDER}/{schema}"
        )

    def tables_path(self, connection_id, database, schema):
        """Build the navigator path of a schema's Tables folder."""
        return f"{self.schema_path(connection_id, database, schema)}/{TABLE_FOLDER}"

    def columns_path(self, connection_id, database, schema, table):
        """Build the navigator path of one table's Columns folder."""
        tables = self.tables_path(connection_id, database, schema)
        return f"{tables}/{table}/{COLUMN_FOLDER}"
