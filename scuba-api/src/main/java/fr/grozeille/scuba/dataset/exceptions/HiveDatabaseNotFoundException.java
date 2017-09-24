package fr.grozeille.scuba.dataset.exceptions;


public class HiveDatabaseNotFoundException extends Exception {
    public HiveDatabaseNotFoundException() {
        super();
    }

    public HiveDatabaseNotFoundException(String message) {
        super(message);
    }

    public HiveDatabaseNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveDatabaseNotFoundException(Throwable cause) {
        super(cause);
    }
}
