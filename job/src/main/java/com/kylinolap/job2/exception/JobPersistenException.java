package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/15/14.
 */
public class JobPersistenException extends Exception {
    private static final long serialVersionUID = -4239863858506718998L;

    public JobPersistenException() {
    }

    public JobPersistenException(String message) {
        super(message);
    }

    public JobPersistenException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobPersistenException(Throwable cause) {
        super(cause);
    }

    public JobPersistenException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
