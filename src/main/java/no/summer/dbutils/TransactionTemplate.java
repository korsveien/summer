package no.summer.dbutils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionTemplate<T> {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionTemplate.class);

    public T execute(TransactionCallbackWithResult<T> callback) {
        if (TestDatabaseTransaction.isInTestTransaction()) {
            //The test will handle transactions (rolled back by after-method in integrationtest).
            //Do not add additional transaction boundry, just execute lambda:
            return callback.doInTransaction();
        }
        else if (ActiveDatabaseTransaction.isInTransaction()) {
            //Participate in transaction
            return callback.doInTransaction();
        }
        else {
            //wrap lambda in a transaction and commit/rollback afterwards:
            try {
                ActiveDatabaseTransaction.initializeTransactionConnection();
                T result = callback.doInTransaction();
                ActiveDatabaseTransaction.commitAndClose();
                return result;
            } catch (Exception e) {
                LOG.error("Exception in database operation", e); //log here just in case rollback / close connection fails
                ActiveDatabaseTransaction.rollbackAndClose();
                throw new RuntimeException("Exception in transaction callback. Transaction rolled back.", e);
            }
        }
    }
}
