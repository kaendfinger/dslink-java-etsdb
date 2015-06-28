package org.etsdb;

public class EtsdbException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public EtsdbException() {
        super();
    }

    public EtsdbException(String message, Throwable cause) {
        super(message, cause);
    }

    public EtsdbException(String message) {
        super(message);
    }

    public EtsdbException(Throwable cause) {
        super(cause);
    }
}
