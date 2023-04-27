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

package org.apache.kylin.tool.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import lombok.val;

public class HiveClientJarToolTestBase {
    @InjectMocks
    protected HiveClientJarTool uploadHiveJarsTool = Mockito.spy(HiveClientJarTool.class);
    @Mock
    protected Appender appender = Mockito.mock(Appender.class);

    protected void before() throws IOException {
        Mockito.when(appender.getName()).thenReturn("mocked");
        Mockito.when(appender.isStarted()).thenReturn(true);
        ((Logger) LogManager.getRootLogger()).addAppender(appender);
    }

    protected void after() throws IOException {
        ((Logger) LogManager.getRootLogger()).removeAppender(appender);
    }

    protected void testExecute(boolean upload, boolean setJarsPath, String message) throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        try {
            if (setJarsPath) {
                config.setProperty("kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path",
                        config.getHdfsWorkingDirectory() + "hive/*");
            }
            config.setProperty("kylin.engine.hive-client-jar-upload.enable", String.valueOf(upload));
            uploadHiveJarsTool.execute();
            ArgumentCaptor<LogEvent> logCaptor = ArgumentCaptor.forClass(LogEvent.class);
            Mockito.verify(appender, Mockito.atLeast(0)).append(logCaptor.capture());
            val logs = logCaptor.getAllValues().stream()
                    .filter(event -> event.getLoggerName().equals("org.apache.kylin.tool.hive.HiveClientJarTool"))
                    .filter(event -> event.getLevel().equals(Level.INFO) || event.getLevel().equals(Level.WARN))
                    .map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
            val logCount = logs.stream().filter(log -> log.equals(message)).count();
            assertEquals(1, logCount);
        } finally {
            config.setProperty("kylin.engine.spark-conf.spark.sql.hive.metastore.jars.path", "");
            config.setProperty("kylin.engine.hive-client-jar-upload.enable", "false");
        }
    }
}
