package org.box1.order.store;

import java.sql.SQLException;

/**
 * A database failure, reported by SQL state only. The driver's exception is deliberately not chained, because its text
 * can hold connection details and row values, and a chained cause reaches logs and dead-letter headers.
 */
public class StoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StoreException(SQLException cause) {
        super("Database operation failed with SQL state " + cause.getSQLState());
    }
}
