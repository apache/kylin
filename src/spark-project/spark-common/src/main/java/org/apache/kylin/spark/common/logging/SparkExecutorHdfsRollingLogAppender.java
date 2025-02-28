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
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
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
import org.apache.spark.SparkEnv;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Plugin(name = "ExecutorHdfsRollingAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SparkExecutorHdfsRollingLogAppender extends AbstractHdfsLogAppender {

    @Getter
    @Setter
    String logPath;

    String executorId;

    boolean logPathInit = false;
    @Getter
    @Setter
    int rollingPeriod = 5;
    @Setter
    @Getter
    private long rollingByteSize;
    @Getter
    @Setter
    private String metadataId;

    @Getter
    @Setter
    private String category;

    @Getter
    @Setter
    private String identifier;

    @Getter
    @Setter
    private String jobName;

    @Getter
    @Setter
    private String project;

    @Getter
    @Setter
    private String jobTimeStamp;

    protected SparkExecutorHdfsRollingLogAppender(String name, Layout<? extends Serializable> layout, Filter filter,
            boolean ignoreExceptions, boolean immediateFlush, Property[] properties, HdfsManager manager) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
    }

    @PluginFactory
    public static SparkExecutorHdfsRollingLogAppender createAppender(@PluginAttribute("name") String name,
            @PluginAttribute("workingDir") String workingDir, @PluginAttribute("metadataId") String metadataId,
            @PluginAttribute("category") String category, @PluginAttribute("identifier") String identifier,
            @PluginAttribute("jobName") String jobName, @PluginAttribute("project") String project,
            @PluginAttribute("jobTimeStamp") String jobTimeStamp, @PluginAttribute("rollingPeriod") int rollingPeriod,
            @PluginAttribute("logQueueCapacity") int logQueueCapacity,
            @PluginAttribute("flushInterval") int flushInterval,
            @PluginAttribute("rollingByteSize") long rollingByteSize,
            @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") Property[] properties) {
        HdfsManager manager = new HdfsManager(name, layout);
        val appender = new SparkExecutorHdfsRollingLogAppender(name, layout, filter, false, false, properties, manager);
        appender.setWorkingDir(workingDir);
        appender.setMetadataId(metadataId);
        appender.setCategory(category);
        appender.setIdentifier(identifier);
        appender.setJobName(jobName);
        appender.setProject(project);
        appender.setRollingByteSize(rollingByteSize);
        if (appender.getRollingByteSize() == 0L) {
            appender.setRollingByteSize(ROLLING_BYTE_SIZE_DEFAULT);
        }
        appender.setJobTimeStamp(jobTimeStamp);
        appender.setRollingPeriod(rollingPeriod);
        appender.setLogQueueCapacity(logQueueCapacity);
        appender.setFlushInterval(flushInterval);
        return appender;
    }

    @Override
    String getAppenderName() {
        return "SparkExecutorHdfsRollingLogAppender";
    }

    @Override
    public void init() {
        StatusLogger.getLogger().warn("metadataIdentifier -> {}", getMetadataId());
        StatusLogger.getLogger().warn("category -> {}", getCategory());
        StatusLogger.getLogger().warn("identifier -> {}", getIdentifier());

        if (null != getProject()) {
            StatusLogger.getLogger().warn("project -> {}", getProject());
        }

        if (null != getJobName()) {
            StatusLogger.getLogger().warn("jobName -> {}", getJobName());
        }
    }

    @Override
    boolean isSkipCheckAndFlushLog() {
        if (SparkEnv.get() == null && StringUtils.isBlank(executorId)) {
            StatusLogger.getLogger().warn("Waiting for spark executor to start");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                StatusLogger.getLogger().error("Waiting for spark executor starting is interrupted!", e);
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }

    @Override
    void doWriteLog(int size, List<LogEvent> transaction) throws IOException, InterruptedException {

        if (!logPathInit) {
            setLogPath(getInitLogPath());
            logPathInit = true;
        }

        // initFileSystemWithToken
        UserGroupInformation ugi = getUGI();
        initFileSystemWithToken(ugi);

        // Check log file size
        if (needRollingFile(getLogPath(), getRollingByteSize())) {
            StatusLogger.getLogger().debug("current log file size > {}, need to rolling", getRollingByteSize());
            // Change the log path & Set outStream = null
            setLogPath(updateOutPutPath(getLogPath()));
        }
        // Check outStream is null
        if (!isWriterInited()) {
            final Path file = new Path(getLogPath());
            // Add tokens to new user so that it may execute its task correctly.
            if (ugi != null) {
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    if (!initHdfsWriter(file, new Configuration())) {
                        StatusLogger.getLogger().error("Failed to init the hdfs writer!");
                    }
                    return null;
                });
            } else {
                if (!initHdfsWriter(file, new Configuration())) {
                    StatusLogger.getLogger().error("Failed to init the hdfs writer!");
                }
            }
        }

        while (size > 0) {
            final LogEvent loggingEvent = getLogBufferQue().take();
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            size--;
        }
    }

    private UserGroupInformation getUGI() throws IOException {
        String sparkuser = System.getenv("SPARK_USER");
        String user = System.getenv("USER");
        StatusLogger.getLogger().warn(" out login user is {} SPARK_USER is {} USER is {}",
                UserGroupInformation.getLoginUser(), sparkuser, user);
        return SparkEnv.getUGI();
    }

    private void initFileSystemWithToken(UserGroupInformation ugi) throws IOException, InterruptedException {
        if (ObjectUtils.isEmpty(ugi)) {
            StatusLogger.getLogger().warn("UserGroupInformation is null");
            return;
        }
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            getFileSystem();
            return null;
        });
    }

    @Override
    String getLogPathAfterRolling(String logPath) {
        Path pathProcess = new Path(logPath);
        return String.format(Locale.ROOT, "%s/executor-%s.%s.log_processing", pathProcess.getParent().toString(),
                executorId, System.currentTimeMillis());
    }

    @Override
    String getLogPathRollingDone(String logPath) {
        return StringUtils.replace(logPath, "_processing", "");
    }

    private String getInitLogPath() {
        if (StringUtils.isBlank(executorId)) {
            executorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : UUID.randomUUID().toString();
            StatusLogger.getLogger().warn("executorId set to {}", executorId);
        }
        switch (getCategory()) {
        case "streaming_job":
            return String.format(Locale.ROOT, "%s/%s/%s/executor-%s.%s.log_processing", getRootPathName(),
                    getIdentifier(), getJobTimeStamp(), executorId, getJobTimeStamp());
        default:
            throw new IllegalArgumentException("illegal category: " + getCategory());
        }
    }

    @VisibleForTesting
    String getRootPathName() {
        switch (getCategory()) {
        case "streaming_job":
            return String.format(Locale.ROOT, "%s/streaming/spark_logs/%s", parseHdfsWordingDir(), getProject());
        default:
            throw new IllegalArgumentException("illegal category: " + getCategory());
        }
    }

    public String getIdentifier() {
        try {
            return StringUtils.isBlank(identifier) ? SparkEnv.get().conf().getAppId() : identifier;
        } catch (Exception e) {
            return null;
        }
    }

    private String parseHdfsWordingDir() {
        return StringUtils.appendIfMissing(getWorkingDir(), "/") + StringUtils.replace(getMetadataId(), "/", "-");
    }

}
