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

package org.apache.kylin.rest.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import lombok.var;

@MetadataInfo
public class SparderUIUtilTest {
    @InjectMocks
    private SparderUIUtil sparderUIUtil;

    @BeforeEach
    public void setUp() {
        sparderUIUtil = Mockito.spy(SparderUIUtil.class);
    }

    @Test
    void testPeriodicGC() {
        Appender appender = Mockito.mock(Appender.class);
        try {
            Mockito.when(appender.getName()).thenReturn("mocked");
            Mockito.when(appender.isStarted()).thenReturn(true);
            ((Logger) LogManager.getRootLogger()).addAppender(appender);

            sparderUIUtil.periodicGC();

            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            var logs = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals(SparderUIUtil.class.getName()))
                    .filter(event -> event.getLevel().equals(Level.INFO))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            assertTrue(CollectionUtils.isNotEmpty(logs));
            assertTrue(logs.stream().anyMatch(log -> log.contains("periodicGC start, crontab is ")));
        } finally {
            ((Logger) LogManager.getRootLogger()).removeAppender(appender);
        }
    }
}
