import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

/**
 * Exercises pgjdbc's extended-protocol features against the fixture and prints a
 * JSON document the Python side (test_jdbc_extended.py) asserts against.
 *
 * Covered: typed PreparedStatement parameter binding (int, string, double,
 * boolean, timestamp, and a NULL bind); ResultSetMetaData; a fetch size inside
 * an explicit transaction (which makes pgjdbc use a server-side cursor / named
 * portal); reuse of one PreparedStatement past pgjdbc's prepareThreshold (which
 * switches it to a server-side named statement); and error recovery after a
 * failed executeUpdate. Each scenario is isolated in its own try/catch so one
 * run reports every result or gap.
 *
 * Usage: java RiffqExtendedHarness <jdbc-url> <user> <password>
 */
public class RiffqExtendedHarness {

    /** Buffer the JSON document is appended to. */
    private final StringBuilder out = new StringBuilder();

    /**
     * Program entry point: open one connection and emit the scenario results.
     *
     * @param args jdbc url, user, and password, in that order
     * @throws Exception if the connection cannot be opened
     */
    public static void main(String[] args) throws Exception {
        String url = args[0];
        Properties props = new Properties();
        props.setProperty("user", args[1]);
        props.setProperty("password", args[2]);
        try (Connection connection = DriverManager.getConnection(url, props)) {
            new RiffqExtendedHarness().run(connection);
        } catch (Exception connectFailure) {
            System.out.println("{\"fatal\": " + quote(connectFailure.toString()) + "}");
            System.exit(1);
        }
    }

    /**
     * Run every scenario against an open connection and print the JSON result.
     *
     * @param connection the open pgjdbc connection
     */
    private void run(Connection connection) {
        out.append("{");
        out.append("\"preparedInt\":");
        preparedFirstColumn(connection, "SELECT name FROM public.customers WHERE id = ?",
                stmt -> stmt.setInt(1, 2));
        out.append(",\"preparedString\":");
        preparedFirstColumn(connection, "SELECT id FROM public.orders WHERE status = ?",
                stmt -> stmt.setString(1, "shipped"));
        out.append(",\"preparedDouble\":");
        preparedFirstColumn(connection, "SELECT sku FROM public.products WHERE price = ?",
                stmt -> stmt.setDouble(1, 9.99));
        out.append(",\"preparedBoolean\":");
        preparedFirstColumn(connection, "SELECT sku FROM public.products WHERE in_stock = ?",
                stmt -> stmt.setBoolean(1, false));
        out.append(",\"preparedTimestamp\":");
        preparedFirstColumn(connection, "SELECT name FROM public.customers WHERE created_at = ?",
                stmt -> stmt.setTimestamp(1, Timestamp.valueOf("2021-01-01 09:00:00")));
        out.append(",\"preparedNull\":");
        preparedFirstColumn(connection,
                "SELECT id FROM public.customers WHERE ? IS NULL ORDER BY id",
                stmt -> stmt.setNull(1, Types.INTEGER));

        out.append(",\"resultSetMetaData\":");
        resultSetMetaData(connection);

        out.append(",\"fetchSizeRows\":");
        fetchSizeInTransaction(connection);

        out.append(",\"statementReuse\":");
        statementReuse(connection);

        out.append(",\"executeUpdateError\":");
        executeUpdateErrorRecovery(connection);

        out.append("}");
        System.out.println(out);
    }

    /**
     * A binder that sets the parameters on a prepared statement.
     */
    private interface Binder {
        /**
         * Bind parameters on the given statement.
         *
         * @param statement the statement to bind onto
         * @throws SQLException if binding fails
         */
        void bind(PreparedStatement statement) throws SQLException;
    }

    /**
     * Run a single-parameter query and append {"rows": [...], "error": ...},
     * where rows are the stringified first column of each result row.
     *
     * @param connection the open connection
     * @param sql the parameterized SQL
     * @param binder sets the parameter value(s)
     */
    private void preparedFirstColumn(Connection connection, String sql, Binder binder) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet rs = statement.executeQuery()) {
                out.append("{\"rows\":[");
                boolean first = true;
                while (rs.next()) {
                    if (!first) {
                        out.append(",");
                    }
                    first = false;
                    Object value = rs.getObject(1);
                    out.append(value == null ? "null" : quote(value.toString()));
                }
                out.append("],\"error\":null}");
            }
        } catch (Exception failure) {
            out.append("{\"rows\":[],\"error\":").append(quote(failure.toString())).append("}");
        }
    }

    /**
     * Append the ResultSetMetaData of a SELECT over customers: per column its
     * name, type name, java.sql.Types code, and nullability.
     *
     * @param connection the open connection
     */
    private void resultSetMetaData(Connection connection) {
        String sql = "SELECT id, name, email, created_at FROM public.customers";
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            out.append("[");
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (i > 1) {
                    out.append(",");
                }
                out.append("{");
                out.append("\"name\":").append(quote(meta.getColumnName(i)));
                out.append(",\"typeName\":").append(quote(meta.getColumnTypeName(i)));
                out.append(",\"type\":").append(meta.getColumnType(i));
                out.append(",\"nullable\":").append(meta.isNullable(i));
                out.append("}");
            }
            out.append("]");
        } catch (Exception failure) {
            out.append("{\"error\":").append(quote(failure.toString())).append("}");
        }
    }

    /**
     * Read customers with a fetch size of two inside an explicit transaction,
     * which makes pgjdbc stream through a server-side cursor rather than
     * buffering the whole result. Append the ordered id column, or an error.
     *
     * @param connection the open connection
     */
    private void fetchSizeInTransaction(Connection connection) {
        boolean originalAutoCommit = true;
        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.setFetchSize(2);
                try (ResultSet rs = statement.executeQuery(
                        "SELECT id FROM public.customers ORDER BY id")) {
                    out.append("[");
                    boolean first = true;
                    while (rs.next()) {
                        if (!first) {
                            out.append(",");
                        }
                        first = false;
                        out.append(rs.getInt(1));
                    }
                    out.append("]");
                }
            }
            connection.commit();
        } catch (Exception failure) {
            out.append("{\"error\":").append(quote(failure.toString())).append("}");
        } finally {
            try {
                connection.setAutoCommit(originalAutoCommit);
            } catch (SQLException ignored) {
                // Restoring autocommit is best effort during teardown.
            }
        }
    }

    /**
     * Execute one PreparedStatement several times with different parameters,
     * crossing pgjdbc's default prepareThreshold (5) so it switches to a
     * server-side named statement. Append the per-execution single result, or an
     * error.
     *
     * @param connection the open connection
     */
    private void statementReuse(Connection connection) {
        String sql = "SELECT name FROM public.customers WHERE id = ?";
        int[] ids = {1, 2, 3, 1, 2, 3, 1};
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            out.append("[");
            for (int i = 0; i < ids.length; i++) {
                if (i > 0) {
                    out.append(",");
                }
                statement.setInt(1, ids[i]);
                try (ResultSet rs = statement.executeQuery()) {
                    out.append(rs.next() ? quote(rs.getString(1)) : "null");
                }
            }
            out.append("]");
        } catch (Exception failure) {
            out.append("{\"error\":").append(quote(failure.toString())).append("}");
        }
    }

    /**
     * Run a failing executeUpdate, then a valid query on the same connection,
     * to confirm the error is clean and does not desynchronize the protocol.
     * Append {"threw": bool, "recovered": bool}.
     *
     * @param connection the open connection
     */
    private void executeUpdateErrorRecovery(Connection connection) {
        boolean threw = false;
        boolean recovered = false;
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE public.no_such_table SET x = 1");
        } catch (SQLException expected) {
            threw = true;
        }
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT count(*) FROM public.customers")) {
            recovered = rs.next() && rs.getInt(1) == 3;
        } catch (Exception failure) {
            recovered = false;
        }
        out.append("{\"threw\":").append(threw).append(",\"recovered\":").append(recovered).append("}");
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
