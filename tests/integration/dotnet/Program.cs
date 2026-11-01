using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;

namespace RiffqNpgsqlHarness;

/// <summary>
/// Drives Npgsql against a riffq fixture server and prints what it saw as JSON.
///
/// Npgsql is a from-scratch implementation of the PostgreSQL wire protocol
/// rather than a libpq wrapper, so it exercises the protocol and the catalog
/// independently of the JDBC and ODBC tiers: it loads its type catalogue from
/// pg_type on connect, and GetSchema issues its own catalog SQL.
///
/// The Python side asserts on the printed document, the same split the Java
/// harnesses use. Per-call failures are captured into "errors" instead of being
/// thrown, so one unsupported call still leaves the rest of the document
/// assertable and the test names the exact call that broke.
/// </summary>
public static class Program
{
    /// <summary>The schema the fixture tables live in.</summary>
    private const string Schema = "public";

    /// <summary>The fixture tables, in the order the Python side expects.</summary>
    private static readonly string[] Tables = { "customers", "orders", "products" };

    /// <summary>Per-call failures, keyed by the call that produced them.</summary>
    private static readonly JsonObject Errors = new();

    /// <summary>
    /// Connect to the fixture server and print the metadata document.
    /// </summary>
    /// <param name="args">Host, port, user, password, and database.</param>
    /// <returns>0 on success; 2 when the arguments are wrong; 1 if connecting
    /// itself failed, which no captured error could describe.</returns>
    public static int Main(string[] args)
    {
        if (args.Length != 5)
        {
            Console.Error.WriteLine(
                "usage: RiffqNpgsqlHarness <host> <port> <user> <password> <database>");
            return 2;
        }

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = args[0],
            Port = int.Parse(args[1]),
            Username = args[2],
            Password = args[3],
            Database = args[4],
            // Deterministic transport: never negotiate TLS, so this tier tests
            // the plain wire path the other tiers use.
            SslMode = SslMode.Disable,
            // One connection per run, so pooling cannot hand back a connection
            // whose state an earlier run established.
            Pooling = false,
        };

        // Type loading is left at its default, so opening the connection runs
        // Npgsql's real startup: a multi-statement batch carrying
        // "SELECT version();" and its pg_type, composite and enum queries.
        // That path is a meaningful part of what this tier covers -- it
        // exercises the catalog far harder than any single query here does.
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(builder.ConnectionString);

        try
        {
            using var dataSource = dataSourceBuilder.Build();
            using var connection = dataSource.OpenConnection();
            Console.WriteLine(Dump(connection).ToJsonString());
            return 0;
        }
        catch (Exception error)
        {
            Console.Error.WriteLine($"connect/dump failed: {error}");
            return 1;
        }
    }

    /// <summary>
    /// Build the full metadata document for an open connection.
    /// </summary>
    /// <param name="connection">The open Npgsql connection to introspect.</param>
    /// <returns>The document the Python side asserts against.</returns>
    private static JsonObject Dump(NpgsqlConnection connection)
    {
        var document = new JsonObject
        {
            ["serverVersion"] = connection.PostgreSqlVersion.ToString(),
            ["npgsqlVersion"] = typeof(NpgsqlConnection).Assembly.GetName().Version?.ToString(),
            ["tables"] = Capture("tables", () => SchemaTables(connection)),
            ["columns"] = Capture("columns", () => SchemaColumns(connection)),
            ["rows"] = Capture("rows", () => AllRows(connection)),
            ["columnTypes"] = Capture("columnTypes", () => CustomerColumnTypes(connection)),
            ["parameterized"] = Capture("parameterized", () => ParameterizedLookup(connection)),
        };
        document["errors"] = Errors;
        return document;
    }

    /// <summary>
    /// Run one section, capturing any failure instead of aborting the dump.
    /// </summary>
    /// <param name="name">The section name, used as the error key.</param>
    /// <param name="section">The section builder to run.</param>
    /// <returns>The section's node, or null when it failed.</returns>
    private static JsonNode Capture(string name, Func<JsonNode> section)
    {
        try
        {
            return section();
        }
        catch (Exception error)
        {
            Errors[name] = error.Message;
            return null;
        }
    }

    /// <summary>
    /// List the fixture schema's tables via Npgsql's own catalog query.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>An array of table names.</returns>
    private static JsonNode SchemaTables(NpgsqlConnection connection)
    {
        var names = new JsonArray();
        var schema = connection.GetSchema("Tables", new[] { null, Schema, null, null });
        foreach (System.Data.DataRow row in schema.Rows)
        {
            names.Add(row["table_name"].ToString());
        }
        return names;
    }

    /// <summary>
    /// Read each fixture table's columns via GetSchema, in ordinal order.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>An object mapping table name to its ordered column names.</returns>
    private static JsonNode SchemaColumns(NpgsqlConnection connection)
    {
        var byTable = new JsonObject();
        foreach (var table in Tables)
        {
            var columns = new JsonArray();
            var schema = connection.GetSchema("Columns", new[] { null, Schema, table, null });
            var ordered = schema.Select(string.Empty, "ordinal_position");
            foreach (System.Data.DataRow row in ordered)
            {
                columns.Add(row["column_name"].ToString());
            }
            byTable[table] = columns;
        }
        return byTable;
    }

    /// <summary>
    /// Read every fixture table's rows, rendering each value as a string.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>An object mapping table name to its rows, ordered by id.</returns>
    private static JsonNode AllRows(NpgsqlConnection connection)
    {
        var byTable = new JsonObject();
        foreach (var table in Tables)
        {
            var rows = new JsonArray();
            using var command = new NpgsqlCommand($"SELECT * FROM {Schema}.{table} ORDER BY id", connection);
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var values = new JsonArray();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    values.Add(reader.IsDBNull(i) ? null : JsonValue.Create(reader.GetValue(i).ToString()));
                }
                rows.Add(values);
            }
            byTable[table] = rows;
        }
        return byTable;
    }

    /// <summary>
    /// Report the CLR type Npgsql maps each customers column to.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>An array of CLR type names in column order.</returns>
    private static JsonNode CustomerColumnTypes(NpgsqlConnection connection)
    {
        var types = new JsonArray();
        using var command = new NpgsqlCommand($"SELECT * FROM {Schema}.customers ORDER BY id", connection);
        using var reader = command.ExecuteReader();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            types.Add(reader.GetFieldType(i).Name);
        }
        return types;
    }

    /// <summary>
    /// Run a parameterized lookup, exercising the extended query protocol.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>An array of the names matching the bound id.</returns>
    private static JsonNode ParameterizedLookup(NpgsqlConnection connection)
    {
        var names = new JsonArray();
        using var command = new NpgsqlCommand(
            $"SELECT name FROM {Schema}.customers WHERE id = @id", connection);
        command.Parameters.AddWithValue("id", 2);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }
        return names;
    }
}
