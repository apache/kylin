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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Before;

import lombok.val;

public class LogOutputTestCase extends NLocalFileMetadataTestCase {

    public static class MockAppender extends AbstractAppender {
        public List<String> events = new ArrayList<>();

        protected MockAppender() {
            super("mock", null, PatternLayout.createDefaultLayout(), true, new Property[0]);
        }

        public void close() {
        }

        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.getMessage().getFormattedMessage());
        }
    }

    @Before
    public void createLoggerAppender() {
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Configuration configuration = context.getConfiguration();
        val loggerAppender = new MockAppender();
        configuration.addAppender(loggerAppender);
        Logger logger = (Logger) LogManager.getLogger("");
        logger.addAppender(configuration.getAppender("mock"));
    }

    @After
    public void removeLoggerAppender() {
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Configuration configuration = context.getConfiguration();
        Logger logger = (Logger) LogManager.getLogger("");
        logger.removeAppender(configuration.getAppender("mock"));
        configuration.removeLogger("mock");
    }

    protected boolean containsLog(String log) {
        return getMockAppender().events.contains(log);
    }

    protected void clearLogs() {
        val appender = getMockAppender();
        if (appender != null) {
            appender.events.clear();
        }
    }

    MockAppender getMockAppender() {
        Logger logger = (Logger) LogManager.getLogger("");
        return (MockAppender) logger.getAppenders().get("mock");
    }
}
