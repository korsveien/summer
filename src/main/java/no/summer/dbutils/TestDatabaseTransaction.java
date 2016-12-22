package no.summer.dbutils;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestDatabaseTransaction {

    private static Connection connection;

    static boolean isInTestTransaction() {
        return connection != null;
    }

    static Connection getConnection() {
        if (connection == null) {
            throw new IllegalStateException("Test transaction not set up");
        }
        return connection;
    }

    public static void setupTestTransaction() {
        setupTestTransactionWithAutocommit(false);
    }

    private static void setupTestTransactionWithAutocommit(boolean autoCommit) {
        try {
            if (connection == null) {
                connection = DriverManager.getConnection("foo", "dbuser", "dbpassword");
                runLiquibaseUpdate();
            }
            connection.setAutoCommit(autoCommit);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not set up test transaction connection", e);
        }
    }

    private static void runLiquibaseUpdate() throws Exception {
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        Liquibase liquibase = new Liquibase("db/changeset.master.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update((String) null);
    }

    public static void rollbackTestTransaction() {
        if (connection == null) {
            throw new IllegalStateException("Test transaction not set up, cannot roll back");
        }

        try {
            connection.rollback();
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not roll back transaction", e);
        }
    }
}
