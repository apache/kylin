package org.apache.kylin.job.exception;

/**
 * Created by qianzhou on 12/26/14.
 */
public class IllegalStateTranferException extends RuntimeException {

    private static final long serialVersionUID = 8466551519300132702L;

    public IllegalStateTranferException() {
    }

    public IllegalStateTranferException(String message) {
        super(message);
    }

    public IllegalStateTranferException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalStateTranferException(Throwable cause) {
        super(cause);
    }

    public IllegalStateTranferException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
