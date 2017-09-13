package org.grozeille.bigdata.dataset.exceptions;


public class HiveTableNotFoundException extends Exception {
    public HiveTableNotFoundException() {
        super();
    }

    public HiveTableNotFoundException(String message) {
        super(message);
    }

    public HiveTableNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveTableNotFoundException(Throwable cause) {
        super(cause);
    }
}
