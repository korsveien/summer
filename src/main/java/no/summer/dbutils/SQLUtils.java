package no.summer.dbutils;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SQLUtils.class);

    public static <T> Optional<T> queryForObject(final String preparedStatement, final RowMapper<T> rowMapper, final Object... args) {
        List<T> results = queryForList(preparedStatement, rowMapper, args);
        if (results.isEmpty()) {
            return Optional.empty();
        }
        else if (results.size() > 1) {
            throw new IncorrectResultSizeException();
        }
        return Optional.of(results.get(0));
    }

    private static <T> List<T> queryForList(final String preparedStatement, final RowMapper<T> rowMapper, final Object... args) {
        return doWithConnection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(preparedStatement)) {
                prepare(statement, args);
                long start = System.currentTimeMillis();
                ResultSet resultSet = statement.executeQuery();
                LOG.debug("Database query took " + (System.currentTimeMillis() - start) + " ms");
                return getResultList(resultSet, rowMapper);
            }
        });
    }

    public static <K, V> Map<K, List<V>> queryForMapOfLists(final String preparedStatement, final MapRowMapper<K, V> rowMapper, final Object... args) {
        return doWithConnection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(preparedStatement)) {
                prepare(statement, args);
                long start = System.currentTimeMillis();
                ResultSet resultSet = statement.executeQuery();
                LOG.debug("Database map query took " + (System.currentTimeMillis() - start) + " ms");
                return getResultMap(resultSet, rowMapper);
            }
        });
    }

    static Connection getConnection() {
        try {
            if (TestDatabaseTransaction.isInTestTransaction()) {
                return TestDatabaseTransaction.getConnection();
            }
            else if (ActiveDatabaseTransaction.isInTransaction()) {
                return ActiveDatabaseTransaction.getTransactionConnection();
            }
            else {
                Context initialContext = new InitialContext();
                DataSource ds = (DataSource)initialContext.lookup("java:comp/env/jdbc/c3p0");
                final Connection connection = ds.getConnection();
                connection.setAutoCommit(true);
                return connection;
            }
        }
        catch (NamingException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Integer executeUpdate(final String preparedStatement, final Object... args) {
        return doWithConnection(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(preparedStatement)) {
                prepare(statement, args);
                return statement.executeUpdate();
            }
        });
    }

    private static <T> T doWithConnection(Callback<T> callback) {
        Connection connection = getConnection();
        try {
            return callback.kjor(connection);
        } catch (SQLException e) {
            LOG.error("Exception in database operation", e); //log here just in case close connection fails
            throw new RuntimeException("Exception in database operation", e);
        } finally {
            closeConnection(connection);
        }
    }

    private static void closeConnection(Connection connection) {
        try {
            if (TestDatabaseTransaction.isInTestTransaction()) {
                //do not close, since we (in the test) want to roll things back
            }
            else if (ActiveDatabaseTransaction.isInTransaction()) {
                //do not close, since we are part of a transaction and want the TransactionTemplate to commit or roll things back before closing
            }
            else {
                //standard case
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not close connection", e);
        }
    }

    private static void prepare(PreparedStatement statement, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            int statementIndex = i + 1;

            if (args[i] == null) {
                statement.setObject(statementIndex, null);
                continue;
            }
            if (args[i] instanceof String) {
                statement.setString(statementIndex, (String) args[i]);
            } else if (args[i] instanceof Character) {
                statement.setString(statementIndex, "" + args[i]);
            } else if (args[i] instanceof Integer) {
                statement.setInt(statementIndex, (Integer) args[i]);
            } else if (args[i] instanceof Long) {
                statement.setLong(statementIndex, (Long) args[i]);
            } else if (args[i] instanceof java.util.Date) {
                statement.setDate(statementIndex, new Date(((java.util.Date) args[i]).getTime()));
            } else if (args[i] instanceof LocalDateTime) {
                LocalDateTime time = ((LocalDateTime) args[i]);
                statement.setTimestamp(statementIndex, Timestamp.valueOf(time));
            } else if (args[i] instanceof LocalDate) {
                LocalDate date = ((LocalDate) args[i]);
                statement.setDate(statementIndex, Date.valueOf(date));
            } else if (args[i] instanceof Boolean) {
                statement.setBoolean(statementIndex, ((Boolean) args[i]));
            } else if (args[i] instanceof Enum) {
                statement.setString(statementIndex, ((Enum) args[i]).name());
            } else {
                throw new RuntimeException("Ukjent type for verdi: " + args[i]);
            }
        }
    }

    private static <T> List<T> getResultList(ResultSet resultSet, final RowMapper<T> rowMapper) throws SQLException {
        try {
            List<T> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(rowMapper.map(resultSet));
            }
            return results;
        } finally {
            resultSet.close();
        }
    }

    private static <K, V> Map<K, List<V>> getResultMap(ResultSet resultSet, final MapRowMapper<K, V> rowMapper) throws SQLException {
        try {
            Map<K, List<V>> results = new HashMap<>();
            while (resultSet.next()) {
                K key = rowMapper.getKey(resultSet);
                V value = rowMapper.getValue(resultSet);

                if (results.containsKey(key)) {
                    results.get(key).add(value);
                }
                else {
                    results.put(key, initArrayList(value));
                }
            }
            return results;
        } finally {
            resultSet.close();
        }
    }

    private interface Callback<T> {
        public T kjor(Connection connection) throws SQLException;
    }

    private static <T> List<T> initArrayList(T a) {
        List<T> result = new ArrayList<>();
        result.add(a);
        return result;
    }
}
