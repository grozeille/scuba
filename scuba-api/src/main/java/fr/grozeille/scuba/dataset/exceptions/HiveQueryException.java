package fr.grozeille.scuba.dataset.exceptions;


public class HiveQueryException extends Exception {
    public HiveQueryException() {
        super();
    }

    public HiveQueryException(String message) {
        super(message);
    }

    public HiveQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveQueryException(Throwable cause) {
        super(cause);
    }
}
