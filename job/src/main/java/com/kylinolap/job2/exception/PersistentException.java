package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/15/14.
 */
public class PersistentException extends Exception {
    private static final long serialVersionUID = -4239863858506718998L;

    public PersistentException() {
    }

    public PersistentException(String message) {
        super(message);
    }

    public PersistentException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistentException(Throwable cause) {
        super(cause);
    }

    public PersistentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
