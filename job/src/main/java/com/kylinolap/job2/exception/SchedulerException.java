package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/15/14.
 */
public class SchedulerException extends Exception {
    private static final long serialVersionUID = 349041244824274861L;

    public SchedulerException() {
    }

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchedulerException(Throwable cause) {
        super(cause);
    }

    public SchedulerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
