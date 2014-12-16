package com.kylinolap.job2.exception;

/**
 * Created by qianzhou on 12/15/14.
 */
public class ExecuteException extends Exception {

    private static final long serialVersionUID = 5677121412192984281L;

    public ExecuteException() {
    }

    public ExecuteException(String message) {
        super(message);
    }

    public ExecuteException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecuteException(Throwable cause) {
        super(cause);
    }
}
