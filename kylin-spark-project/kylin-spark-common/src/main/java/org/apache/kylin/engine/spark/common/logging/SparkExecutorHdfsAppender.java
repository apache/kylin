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

package org.apache.kylin.engine.spark.common.logging;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.SparkHadoopUtil;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class SparkExecutorHdfsAppender extends AbstractHdfsLogAppender {

    private static final long A_DAY_MILLIS = 24 * 60 * 60 * 1000L;
    private static final long A_HOUR_MILLIS = 60 * 60 * 1000L;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
    private SimpleDateFormat hourFormat = new SimpleDateFormat("HH", Locale.ROOT);

    @VisibleForTesting
    String outPutPath;
    @VisibleForTesting
    String executorId;

    @VisibleForTesting
    long startTime = 0;
    @VisibleForTesting
    boolean rollingByHour = false;
    @VisibleForTesting
    int rollingPeriod = 5;

    //log appender configurable
    private String metadataIdentifier;
    private String category;

    private String identifier;

    // only cubing job
    private String jobName;
    private String project;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public void setMetadataIdentifier(String metadataIdentifier) {
        this.metadataIdentifier = metadataIdentifier;
    }

    public String getMetadataIdentifier() {
        return metadataIdentifier;
    }

    @Override
    void init() {
        LogLog.warn("metadataIdentifier -> " + getMetadataIdentifier());
        LogLog.warn("category -> " + getCategory());
        LogLog.warn("identifier -> " + getIdentifier());

        if (null != getProject()) {
            LogLog.warn("project -> " + getProject());
        }

        if (null != getJobName()) {
            LogLog.warn("jobName -> " + getJobName());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkExecutorHdfsAppender";
    }

    @Override
    boolean isSkipCheckAndFlushLog() {
        if (SparkEnv.get() == null && StringUtils.isBlank(executorId)) {
            LogLog.warn("Waiting for spark executor to start");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LogLog.error("Waiting for spark executor starting is interrupted!", e);
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }

    @Override
    void doWriteLog(int size, List<LoggingEvent> transaction) throws IOException, InterruptedException {
        while (size > 0) {
            final LoggingEvent loggingEvent = getLogBufferQue().take();
            if (isTimeChanged(loggingEvent)) {
                updateOutPutDir(loggingEvent);

                final Path file = new Path(outPutPath);

                String sparkuser = System.getenv("SPARK_USER");
                String user = System.getenv("USER");
                LogLog.warn("login user is " + UserGroupInformation.getLoginUser() + " SPARK_USER is " + sparkuser
                        + " USER is " + user);
                SparkHadoopUtil.get().runAsSparkUser(new scala.runtime.AbstractFunction0<scala.runtime.BoxedUnit>() {
                    @Override
                    public BoxedUnit apply() {
                        if (!initHdfsWriter(file, new Configuration())) {
                            LogLog.error("Failed to init the hdfs writer!");
                        }
                        try {
                            doRollingClean(loggingEvent);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                });
            }
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            size--;
        }
    }

    @VisibleForTesting
    void updateOutPutDir(LoggingEvent event) {
        if (rollingByHour) {
            String rollingDir = dateFormat.format(new Date(event.getTimeStamp())) + "/"
                    + hourFormat.format(new Date(event.getTimeStamp()));
            outPutPath = getOutPutDir(rollingDir);
        } else {
            String rollingDir = dateFormat.format(new Date(event.getTimeStamp()));
            outPutPath = getOutPutDir(rollingDir);
        }
        LogLog.warn("Update to " + outPutPath);
    }

    private String getOutPutDir(String rollingDir) {
        if (StringUtils.isBlank(executorId)) {
            executorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : UUID.randomUUID().toString();
            LogLog.warn("executorId set to " + executorId);
        }

        if ("job".equals(getCategory())) {
            return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + getJobName() + "/" + "executor-"
                    + executorId + ".log";
        }
        return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + "executor-" + executorId + ".log";
    }

    @VisibleForTesting
    void doRollingClean(LoggingEvent event) throws IOException {
        FileSystem fileSystem = getFileSystem();

        String rootPathName = getRootPathName();
        Path rootPath = new Path(rootPathName);

        if (!fileSystem.exists(rootPath))
            return;

        FileStatus[] logFolders = fileSystem.listStatus(rootPath);

        if (logFolders == null)
            return;

        String thresholdDay = dateFormat.format(new Date(event.getTimeStamp() - A_DAY_MILLIS * rollingPeriod));

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
        if ("job".equals(getCategory())) {
            return getHdfsWorkingDir() + "/" + getProject() + "/spark_logs/executor/";
        } else if ("sparder".equals(getCategory())) {
            return parseHdfsWorkingDir() + "/_sparder_logs";
        } else {
            throw new IllegalArgumentException("illegal category: " + getCategory());
        }
    }

    @VisibleForTesting
    boolean isTimeChanged(LoggingEvent event) {
        if (rollingByHour) {
            return isNeedRolling(event, A_HOUR_MILLIS);
        } else {
            return isNeedRolling(event, A_DAY_MILLIS);
        }
    }

    private boolean isNeedRolling(LoggingEvent event, Long timeInterval) {
        if (0 == startTime || ((event.getTimeStamp() / timeInterval) - (startTime / timeInterval)) > 0) {
            startTime = event.getTimeStamp();
            return true;
        }
        return false;
    }

    private String parseHdfsWorkingDir() {
        String root = getHdfsWorkingDir();
        Path path = new Path(root);
        if (!path.isAbsolute())
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);

        try {
            FileSystem fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration());
            path = fs.makeQualified(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // append metadata-url prefix
        String metaId = getMetadataIdentifier().replace(':', '-');
        //transform relative path for local metadata
        if (metaId.startsWith("../")) {
            metaId = metaId.replace("../", "");
            metaId = metaId.replace('/', '-');
        }

        root = new Path(path, metaId).toString();

        if (!root.endsWith("/"))
            root += "/";
        return root;
    }
}

