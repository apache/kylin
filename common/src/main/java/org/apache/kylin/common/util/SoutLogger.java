package org.apache.kylin.common.util;

/**
 * Created by Hongbin Ma(Binmahone) on 2/6/15.
 */
public class SoutLogger implements Logger {

    @Override
    public void log(String message) {
        System.out.println(message);
    }
}
