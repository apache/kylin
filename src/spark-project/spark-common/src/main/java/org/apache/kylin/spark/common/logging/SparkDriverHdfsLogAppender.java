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
package org.apache.kylin.spark.common.logging;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.status.StatusLogger;

import lombok.Getter;
import lombok.Setter;

@Plugin(name = "DriverHdfsAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SparkDriverHdfsLogAppender extends AbstractHdfsLogAppender {

    private static SparkDriverHdfsLogAppender appender;
    @Getter
    @Setter
    private String logPath;
    // kerberos
    @Getter
    @Setter
    private boolean kerberosEnabled = false;
    @Getter
    @Setter
    private String kerberosPrincipal;
    @Getter
    @Setter
    private String kerberosKeytab;

    protected SparkDriverHdfsLogAppender(String name, Layout<? extends Serializable> layout, Filter filter,
            boolean ignoreExceptions, boolean immediateFlush, Property[] properties, HdfsManager manager) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
    }

    @PluginFactory
    public synchronized static SparkDriverHdfsLogAppender createAppender(@PluginAttribute("name") String name,
            @PluginAttribute("kerberosEnabled") boolean kerberosEnabled,
            @PluginAttribute("kerberosPrincipal") String kerberosPrincipal,
            @PluginAttribute("kerberosKeytab") String kerberosKeytab,
            @PluginAttribute("workingDir") String hdfsWorkingDir, @PluginAttribute("logPath") String logPath,
            @PluginAttribute("logQueueCapacity") int logQueueCapacity,
            @PluginAttribute("flushInterval") int flushInterval,
            @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") Property[] properties) {
        if (appender != null) {
            return appender;
        }
        HdfsManager manager = new HdfsManager(name, layout);
        appender = new SparkDriverHdfsLogAppender(name, layout, filter, false, false, properties, manager);
        appender.setKerberosEnabled(kerberosEnabled);
        if (kerberosEnabled) {
            appender.setKerberosPrincipal(kerberosPrincipal);
            appender.setKerberosKeytab(kerberosKeytab);
        }
        appender.setWorkingDir(hdfsWorkingDir);
        appender.setLogPath(logPath);
        appender.setLogQueueCapacity(logQueueCapacity);
        appender.setFlushInterval(flushInterval);
        return appender;
    }

    @Override
    public void init() {
        StatusLogger.getLogger().warn("spark.driver.log4j.appender.hdfs.File -> " + getLogPath());
        StatusLogger.getLogger().warn("kerberosEnable -> " + isKerberosEnabled());
        if (isKerberosEnabled()) {
            StatusLogger.getLogger().warn("kerberosPrincipal -> " + getKerberosPrincipal());
            StatusLogger.getLogger().warn("kerberosKeytab -> " + getKerberosKeytab());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkDriverHdfsLogAppender";
    }

    @Override
    public boolean isSkipCheckAndFlushLog() {
        return false;
    }

    @Override
    public void doWriteLog(int eventSize, List<LogEvent> transaction) throws IOException, InterruptedException {
        if (!isWriterInited()) {
            Configuration conf = new Configuration();
            if (!initHdfsWriter(new Path(getLogPath()), conf)) {
                StatusLogger.getLogger().error("init the hdfs writer failed!");
            }
        }

        while (eventSize > 0) {
            LogEvent loggingEvent = getLogBufferQue().take();
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            eventSize--;
        }
    }

    @Override
    String getLogPathAfterRolling(String logPath) {
        return null;
    }

    @Override
    String getLogPathRollingDone(String logPath) {
        return null;
    }

}
