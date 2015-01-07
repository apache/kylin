package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/17/14.
 */
public class LockException extends Exception {
    private static final long serialVersionUID = 2072745879281754945L;

    public LockException() {
    }

    public LockException(String message) {
        super(message);
    }

    public LockException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockException(Throwable cause) {
        super(cause);
    }

    public LockException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
