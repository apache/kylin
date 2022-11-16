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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.RandomUtil;
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
import org.apache.spark.utils.SparkHadoopUtils;

import com.google.common.annotations.VisibleForTesting;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Plugin(name = "ExecutorHdfsAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SparkExecutorHdfsLogAppender extends AbstractHdfsLogAppender {

    private static final long A_DAY_MILLIS = 24 * 60 * 60 * 1000L;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd",
            Locale.getDefault(Locale.Category.FORMAT));

    @VisibleForTesting
    String outputPath;
    @VisibleForTesting
    String executorId;

    @VisibleForTesting
    long startTime = 0;

    @Getter
    @Setter
    @VisibleForTesting
    int rollingPeriod = 5;

    //log appender configurable
    @Getter
    @Setter
    private String metadataId;
    @Getter
    @Setter
    private String category;
    @Getter
    @Setter
    private String identifier;

    // only cubing job
    @Getter
    @Setter
    private String jobName;
    @Getter
    @Setter
    private String project;

    protected SparkExecutorHdfsLogAppender(String name, Layout<? extends Serializable> layout, Filter filter,
            boolean ignoreExceptions, boolean immediateFlush, Property[] properties, HdfsManager manager) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
    }

    @PluginFactory
    public static SparkExecutorHdfsLogAppender createAppender(@PluginAttribute("name") String name,
            @PluginAttribute("workingDir") String workingDir, @PluginAttribute("metadataId") String metadataId,
            @PluginAttribute("category") String category, @PluginAttribute("identifier") String identifier,
            @PluginAttribute("jobName") String jobName, @PluginAttribute("project") String project,
            @PluginAttribute("rollingPeriod") int rollingPeriod,
            @PluginAttribute("logQueueCapacity") int logQueueCapacity,
            @PluginAttribute("flushInterval") int flushInterval,
            @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") Property[] properties) {
        HdfsManager manager = new HdfsManager(name, layout);
        val appender = new SparkExecutorHdfsLogAppender(name, layout, filter, false, false, properties, manager);
        appender.setWorkingDir(workingDir);
        appender.setMetadataId(metadataId);
        appender.setCategory(category);
        appender.setIdentifier(identifier);
        appender.setJobName(jobName);
        appender.setProject(project);
        appender.setRollingPeriod(rollingPeriod);
        appender.setLogQueueCapacity(logQueueCapacity);
        appender.setFlushInterval(flushInterval);
        return appender;
    }

    @Override
    void init() {
        StatusLogger.getLogger().warn("metadataIdentifier -> " + getMetadataId());
        StatusLogger.getLogger().warn("category -> " + getCategory());
        StatusLogger.getLogger().warn("identifier -> " + getIdentifier());

        if (null != getProject()) {
            StatusLogger.getLogger().warn("project -> " + getProject());
        }

        if (null != getJobName()) {
            StatusLogger.getLogger().warn("jobName -> " + getJobName());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkExecutorHdfsLogAppender";
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
        while (size > 0) {
            final LogEvent loggingEvent = getLogBufferQue().take();
            if (isTimeChanged(loggingEvent)) {
                updateOutPutDir(loggingEvent);

                final Path file = new Path(outputPath);

                String sparkuser = System.getenv("SPARK_USER");
                String user = System.getenv("USER");
                StatusLogger.getLogger().warn("login user is " + UserGroupInformation.getLoginUser() + " SPARK_USER is "
                        + sparkuser + " USER is " + user);
                UserGroupInformation ugi = SparkEnv.getUGI();
                // Add tokens to new user so that it may execute its task correctly.
                if (ugi != null) {
                    StatusLogger.getLogger().warn("Login user hashcode is " + ugi.hashCode());
                    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                        if (!initHdfsWriter(file, SparkHadoopUtils.newConfigurationWithSparkConf())) {
                            StatusLogger.getLogger().error("Failed to init the hdfs writer!");
                        }
                        doRollingClean(loggingEvent);
                        return null;
                    });
                } else {
                    if (!initHdfsWriter(file, SparkHadoopUtils.newConfigurationWithSparkConf())) {
                        StatusLogger.getLogger().error("Failed to init the hdfs writer!");
                    }
                    doRollingClean(loggingEvent);
                }
            }

            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            size--;
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

    @VisibleForTesting
    void updateOutPutDir(LogEvent event) {
        String rollingDir = dateFormat.format(new Date(event.getTimeMillis()));
        outputPath = getOutPutDir(rollingDir);
    }

    private String getOutPutDir(String rollingDir) {
        if (StringUtils.isBlank(executorId)) {
            executorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : RandomUtil.randomUUIDStr();
            StatusLogger.getLogger().warn("executorId set to " + executorId);
        }

        if ("job".equals(getCategory())) {
            return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + getJobName() + "/" + "executor-"
                    + executorId + ".log";
        }
        return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + "executor-" + executorId + ".log";
    }

    @VisibleForTesting
    void doRollingClean(LogEvent event) throws IOException {
        FileSystem fileSystem = getFileSystem();

        String rootPathName = getRootPathName();
        Path rootPath = new Path(rootPathName);

        if (!fileSystem.exists(rootPath))
            return;

        FileStatus[] logFolders = fileSystem.listStatus(rootPath);

        if (logFolders == null)
            return;

        String thresholdDay = dateFormat.format(new Date(event.getTimeMillis() - A_DAY_MILLIS * rollingPeriod));

        for (FileStatus fs : logFolders) {
            String fileName = fs.getPath().getName();
            if (fileName.compareTo(thresholdDay) < 0) {
                Path fullPath = new Path(rootPathName + File.separator + fileName);
                if (!fileSystem.exists(fullPath))
                    continue;
                fileSystem.delete(fullPath, true);
            }
        }
    }

    @VisibleForTesting
    String getRootPathName() {
        switch (getCategory()) {
        case "job":
            return parseHdfsWordingDir() + "/" + getProject() + "/spark_logs";
        case "sparder":
            return parseHdfsWordingDir() + "/_sparder_logs";
        case "streaming_job":
            return parseHdfsWordingDir() + "/streaming/spark_logs/" + getProject();
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

    @VisibleForTesting
    boolean isTimeChanged(LogEvent event) {
        if (0 == startTime || ((event.getTimeMillis() / A_DAY_MILLIS) - (startTime / A_DAY_MILLIS)) > 0) {
            startTime = event.getTimeMillis();
            return true;
        }
        return false;
    }

    private String parseHdfsWordingDir() {
        return StringUtils.appendIfMissing(getWorkingDir(), "/") + StringUtils.replace(getMetadataId(), "/", "-");
    }

}
