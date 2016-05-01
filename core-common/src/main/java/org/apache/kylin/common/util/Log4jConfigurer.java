/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.common.util;

import java.io.InputStream;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by dongli on 11/24/15.
 */
public class Log4jConfigurer {
    private static boolean INITIALIZED = false;

    public static void initLogger() {
        if (!INITIALIZED && !isConfigured()) {
            InputStream is = Log4jConfigurer.class.getClassLoader().getResourceAsStream("kylin-log4j.properties");
            PropertyConfigurator.configure(is);
            IOUtils.closeQuietly(is);
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
