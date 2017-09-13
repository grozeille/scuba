package org.grozeille.bigdata.dataset.exceptions;

public class HiveInvalidDataSetException extends HiveQueryException {
    public HiveInvalidDataSetException() { super(); }
    public HiveInvalidDataSetException(String message) { super(message); }
    public HiveInvalidDataSetException(String message, Throwable cause) { super(message, cause); }
    public HiveInvalidDataSetException(Throwable cause) { super(cause); }
}