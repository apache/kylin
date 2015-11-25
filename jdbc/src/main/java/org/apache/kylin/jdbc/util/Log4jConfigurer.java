package org.apache.kylin.jdbc.util;

import java.util.Enumeration;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Created by dongli on 11/24/15.
 */
public class Log4jConfigurer {
    private final static String DEFAULT_PATTERN_LAYOUT = "L4J [%d{yyyy-MM-dd HH:mm:ss,SSS}][%p][%c] - %m%n";
    private static boolean INITIALIZED = false;

    public static void initLogger() {
        if (!INITIALIZED && !isConfigured()) {
            org.apache.log4j.BasicConfigurator.configure(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)));
        }
        INITIALIZED = true;
    }

    private static boolean isConfigured() {
        if (LogManager.getRootLogger().getAllAppenders().hasMoreElements()) {
            return true;
        } else {
            Enumeration<?> loggers = LogManager.getCurrentLoggers();
            while (loggers.hasMoreElements()) {
                Logger logger = (Logger) loggers.nextElement();
                if (logger.getAllAppenders().hasMoreElements())
                    return true;
            }
        }
        return false;
    }
}
