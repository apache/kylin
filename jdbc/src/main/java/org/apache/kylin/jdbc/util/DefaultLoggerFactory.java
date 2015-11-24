package org.apache.kylin.jdbc.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dongli on 11/23/15.
 */
public class DefaultLoggerFactory {

    private final static String DEFAULT_PATTERN_LAYOUT = "L4J [%d{yyyy-MM-dd HH:mm:ss,SSS}][%p][%c] - %m%n";

    public static Logger getLogger(Class<?> clazz) {
        org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(clazz);
        if (log4jLogger != null && !log4jLogger.getAllAppenders().hasMoreElements())
            log4jLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)));
        return LoggerFactory.getLogger(clazz);
    }
}
