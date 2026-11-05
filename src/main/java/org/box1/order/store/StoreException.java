package org.box1.order.store;

import java.sql.SQLException;

/** A database failure. Message text comes from the driver, so it is logged by SQL state only. */
public class StoreException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public StoreException(SQLException cause) {
        super("Database operation failed with SQL state " + cause.getSQLState(), cause);
    }
}
