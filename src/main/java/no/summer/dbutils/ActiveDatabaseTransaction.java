package no.summer.dbutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class ActiveDatabaseTransaction {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveDatabaseTransaction.class);

    private static ThreadLocal<Connection> activeConnection;

    static {
        activeConnection = new ThreadLocal<>();
    }

    static void initializeTransactionConnection() {
        if (isInTransaction()) {
            throw new RuntimeException("Transaction for current thread was already active! (please use TransactionTemplate to handle nested transactions)");
        }

        Connection connection = SQLUtils.getConnection();
        try {
            connection.setAutoCommit(false);
            activeConnection.set(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static void commitAndClose() {
        Connection connection = getConnectionFailFast();
        try {
            connection.commit();
        } catch (SQLException e) {
            LOG.error("Exception in database operation", e); //log here just in case close connection fails
            throw new RuntimeException("Could not commit transaction", e);
        }
        finally {
            safeClose(connection);
        }
    }

    static void rollbackAndClose() {
        Connection connection = getConnectionFailFast();
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.error("Exception in database operation", e); //log here just in case close connection fails
            throw new RuntimeException("Could not roll back transaction", e);
        }
        finally {
            safeClose(connection);
        }
    }

    private static void safeClose(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            activeConnection.remove();
        }
    }

    static Connection getTransactionConnection() {
        return getConnectionFailFast();
    }

    static boolean isInTransaction() {
        return activeConnection.get() != null;
    }

    private static Connection getConnectionFailFast() {
        Connection connection = activeConnection.get();
        if (connection == null) {
            throw new IllegalStateException("Not in active transaction");
        }
        return connection;
    }

}
