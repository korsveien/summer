package no.summer.dbutils;

public interface TransactionCallbackWithResult<T> {

    T doInTransaction();
}
