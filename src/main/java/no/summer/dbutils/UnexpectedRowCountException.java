package no.summer.dbutils;

/**
 * Unexpected number of rows affected during after a database write operation (typically more than one).
 */
public class UnexpectedRowCountException extends RuntimeException {
    public UnexpectedRowCountException(String s) {
        super(s);
    }
}
