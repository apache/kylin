package org.apache.kylin.common.util;

/**
 */
public class SoutLogger implements Logger {

    @Override
    public void log(String message) {
        System.out.println(message);
    }
}
