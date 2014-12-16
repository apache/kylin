package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/15/14.
 */
public class SchedularException extends Exception {
    private static final long serialVersionUID = 349041244824274861L;

    public SchedularException() {
    }

    public SchedularException(String message) {
        super(message);
    }

    public SchedularException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchedularException(Throwable cause) {
        super(cause);
    }

    public SchedularException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
