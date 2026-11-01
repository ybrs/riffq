import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

/**
 * Dumps a pgjdbc connection's catalog metadata to stdout as a single JSON
 * document, for the Python side (test_jdbc_metadata.py) to assert against.
 *
 * The point of driving this through the real pgjdbc DatabaseMetaData API rather
 * than hand-written SQL is that pgjdbc issues its own catalog queries -- exactly
 * the ones a catalog-emulation layer can get wrong. Each metadata call is run in
 * its own try/catch so one invocation surfaces every gap at once: a call that
 * throws is recorded under "errors" instead of aborting the dump.
 *
 * Usage: java RiffqMetadataHarness <jdbc-url> <user> <password>
 */
public class RiffqMetadataHarness {

    /** The schema the fixture tables live in; pgjdbc filters metadata on it. */
    private static final String SCHEMA = "public";

    /** Buffer the whole JSON document is appended to. */
    private final StringBuilder out = new StringBuilder();

    /** Records the first-seen error message per failing metadata call. */
    private final StringBuilder errors = new StringBuilder();

    /** True once at least one error has been appended, for JSON comma control. */
    private boolean anyError = false;

    /**
     * Program entry point: open one connection and emit the metadata dump.
     *
     * @param args jdbc url, user, and password, in that order
     * @throws Exception if the connection itself cannot be opened; a connection
     *     failure is fatal and printed as a JSON object with a "fatal" key
     */
    public static void main(String[] args) throws Exception {
        String url = args[0];
        Properties props = new Properties();
        props.setProperty("user", args[1]);
        props.setProperty("password", args[2]);
        try (Connection connection = DriverManager.getConnection(url, props)) {
            new RiffqMetadataHarness().dump(connection);
        } catch (Exception connectFailure) {
            System.out.println("{\"fatal\": " + quote(connectFailure.toString()) + "}");
            System.exit(1);
        }
    }

    /**
     * Write the full metadata document for an open connection to stdout.
     *
     * @param connection the open pgjdbc connection to introspect
     * @throws Exception only if writing the identity fields fails; per-table and
     *     per-call failures are captured, not thrown
     */
    private void dump(Connection connection) throws Exception {
        DatabaseMetaData meta = connection.getMetaData();
        out.append("{");
        field("productName", meta.getDatabaseProductName());
        out.append(",");
        field("productVersion", meta.getDatabaseProductVersion());
        out.append(",");
        field("driverVersion", meta.getDriverVersion());

        out.append(",\"catalogs\":");
        appendSingleColumn(meta.getCatalogs(), "TABLE_CAT");

        out.append(",\"schemas\":");
        appendSingleColumn(meta.getSchemas(), "TABLE_SCHEM");

        out.append(",\"tables\":");
        appendTables(meta);

        out.append(",\"columns\":");
        appendColumns(meta);

        out.append(",\"primaryKeys\":");
        appendPerTableCounts(meta, "primaryKeys");

        out.append(",\"importedKeys\":");
        appendPerTableCounts(meta, "importedKeys");

        out.append(",\"indexInfo\":");
        appendPerTableCounts(meta, "indexInfo");

        out.append(",\"typeInfo\":");
        appendTypeInfo(meta);

        out.append(",\"rows\":");
        appendRows(connection);

        out.append(",\"errors\":{").append(errors).append("}");
        out.append("}");
        System.out.println(out);
    }

    /**
     * Append the tables in the fixture schema as an array of objects with their
     * catalog, schema, name, and type.
     *
     * @param meta the connection's metadata handle
     */
    private void appendTables(DatabaseMetaData meta) {
        try (ResultSet rs = meta.getTables(null, SCHEMA, "%", new String[] {"TABLE"})) {
            out.append("[");
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    out.append(",");
                }
                first = false;
                out.append("{");
                field("cat", rs.getString("TABLE_CAT"));
                out.append(",");
                field("schema", rs.getString("TABLE_SCHEM"));
                out.append(",");
                field("name", rs.getString("TABLE_NAME"));
                out.append(",");
                field("type", rs.getString("TABLE_TYPE"));
                out.append("}");
            }
            out.append("]");
        } catch (Exception failure) {
            out.append("[]");
            recordError("tables", failure);
        }
    }

    /**
     * Append a map of table name to its column descriptors (name, ordinal,
     * data type, type name, nullability).
     *
     * @param meta the connection's metadata handle
     */
    private void appendColumns(DatabaseMetaData meta) {
        out.append("{");
        boolean firstTable = true;
        for (String table : new String[] {"customers", "orders", "products"}) {
            if (!firstTable) {
                out.append(",");
            }
            firstTable = false;
            out.append(quote(table)).append(":");
            try (ResultSet rs = meta.getColumns(null, SCHEMA, table, "%")) {
                out.append("[");
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        out.append(",");
                    }
                    first = false;
                    out.append("{");
                    field("name", rs.getString("COLUMN_NAME"));
                    out.append(",");
                    field("ordinal", rs.getInt("ORDINAL_POSITION"));
                    out.append(",");
                    field("dataType", rs.getInt("DATA_TYPE"));
                    out.append(",");
                    field("typeName", rs.getString("TYPE_NAME"));
                    out.append(",");
                    field("nullable", rs.getInt("NULLABLE"));
                    out.append("}");
                }
                out.append("]");
            } catch (Exception failure) {
                out.append("[]");
                recordError("columns:" + table, failure);
            }
        }
        out.append("}");
    }

    /**
     * Append the row count each of getPrimaryKeys / getImportedKeys /
     * getIndexInfo returns per table, or record the error if the call throws.
     * These are expected to be empty (the fixture has no keys or indexes); the
     * count lets the test assert "empty, not error".
     *
     * @param meta the connection's metadata handle
     * @param which one of "primaryKeys", "importedKeys", "indexInfo"
     */
    private void appendPerTableCounts(DatabaseMetaData meta, String which) {
        out.append("{");
        boolean firstTable = true;
        for (String table : new String[] {"customers", "orders", "products"}) {
            if (!firstTable) {
                out.append(",");
            }
            firstTable = false;
            out.append(quote(table)).append(":");
            try (ResultSet rs = openKeyResultSet(meta, which, table)) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                out.append(count);
            } catch (Exception failure) {
                out.append(-1);
                recordError(which + ":" + table, failure);
            }
        }
        out.append("}");
    }

    /**
     * Open the ResultSet for one of the key/index metadata calls.
     *
     * @param meta the connection's metadata handle
     * @param which one of "primaryKeys", "importedKeys", "indexInfo"
     * @param table the table to introspect
     * @return the metadata ResultSet for that call
     * @throws Exception if the underlying metadata query fails
     */
    private ResultSet openKeyResultSet(DatabaseMetaData meta, String which, String table)
            throws Exception {
        if (which.equals("primaryKeys")) {
            return meta.getPrimaryKeys(null, SCHEMA, table);
        }
        if (which.equals("importedKeys")) {
            return meta.getImportedKeys(null, SCHEMA, table);
        }
        return meta.getIndexInfo(null, SCHEMA, table, false, false);
    }

    /**
     * Append the type names getTypeInfo reports, as a JSON array of strings.
     *
     * @param meta the connection's metadata handle
     */
    private void appendTypeInfo(DatabaseMetaData meta) {
        try (ResultSet rs = meta.getTypeInfo()) {
            out.append("[");
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    out.append(",");
                }
                first = false;
                out.append(quote(rs.getString("TYPE_NAME")));
            }
            out.append("]");
        } catch (Exception failure) {
            out.append("[]");
            recordError("typeInfo", failure);
        }
    }

    /**
     * Append the data rows of each table as a map of table name to a list of
     * stringified row values, so the test can compare row content.
     *
     * @param connection the open connection to query through
     */
    private void appendRows(Connection connection) {
        out.append("{");
        boolean firstTable = true;
        for (String table : new String[] {"customers", "orders", "products"}) {
            if (!firstTable) {
                out.append(",");
            }
            firstTable = false;
            out.append(quote(table)).append(":");
            String sql = "SELECT * FROM " + SCHEMA + "." + table + " ORDER BY id";
            try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
                ResultSetMetaData rowMeta = rs.getMetaData();
                int columnCount = rowMeta.getColumnCount();
                out.append("[");
                boolean firstRow = true;
                while (rs.next()) {
                    if (!firstRow) {
                        out.append(",");
                    }
                    firstRow = false;
                    out.append("[");
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) {
                            out.append(",");
                        }
                        Object value = rs.getObject(i);
                        out.append(value == null ? "null" : quote(value.toString()));
                    }
                    out.append("]");
                }
                out.append("]");
            } catch (Exception failure) {
                out.append("[]");
                recordError("rows:" + table, failure);
            }
        }
        out.append("}");
    }

    /**
     * Append a JSON array of the single named column of a metadata ResultSet.
     *
     * @param rs the metadata ResultSet (closed here)
     * @param column the column name to read from each row
     */
    private void appendSingleColumn(ResultSet rs, String column) {
        try (ResultSet closeable = rs) {
            out.append("[");
            boolean first = true;
            while (closeable.next()) {
                if (!first) {
                    out.append(",");
                }
                first = false;
                out.append(quote(closeable.getString(column)));
            }
            out.append("]");
        } catch (Exception failure) {
            out.append("[]");
            recordError(column, failure);
        }
    }

    /**
     * Append a JSON key/value pair with a string value to the main buffer.
     *
     * @param key the JSON key
     * @param value the string value (may be null)
     */
    private void field(String key, String value) {
        out.append(quote(key)).append(":").append(value == null ? "null" : quote(value));
    }

    /**
     * Append a JSON key/value pair with an integer value to the main buffer.
     *
     * @param key the JSON key
     * @param value the integer value
     */
    private void field(String key, int value) {
        out.append(quote(key)).append(":").append(value);
    }

    /**
     * Record the message of a failing metadata call under its label.
     *
     * @param label identifies which call failed (for example "primaryKeys:orders")
     * @param failure the exception the call threw
     */
    private void recordError(String label, Exception failure) {
        if (anyError) {
            errors.append(",");
        }
        anyError = true;
        errors.append(quote(label)).append(":").append(quote(failure.toString()));
    }

    /**
     * Return a JSON string literal for a value, with the required characters
     * escaped.
     *
     * @param value the raw string (may be null)
     * @return a quoted, escaped JSON string, or the literal null
     */
    private static String quote(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder builder = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
            }
        }
        return builder.append("\"").toString();
    }
}
