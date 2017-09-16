package org.grozeille.bigdata.dataset.exceptions;


public class HiveTableAlreadyExistsException extends Exception {
    public HiveTableAlreadyExistsException() {
        super();
    }

    public HiveTableAlreadyExistsException(String message) {
        super(message);
    }

    public HiveTableAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveTableAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}
