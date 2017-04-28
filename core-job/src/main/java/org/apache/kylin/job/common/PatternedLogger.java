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

package org.apache.kylin.job.common;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

/**
 * A logger which parses certain patterns from log
 */
public class PatternedLogger extends BufferedLogger {
    private final Map<String, String> info = Maps.newHashMap();

    private static final Pattern PATTERN_APP_ID = Pattern.compile("Submitted application (.*?) to ResourceManager");
    private static final Pattern PATTERN_APP_URL = Pattern.compile("The url to track the job: (.*)");
    private static final Pattern PATTERN_JOB_ID = Pattern.compile("Running job: (.*)");
    private static final Pattern PATTERN_HDFS_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS: Number of bytes written=(\\d+)");
    private static final Pattern PATTERN_SOURCE_RECORDS_COUNT = Pattern.compile("Map input records=(\\d+)");
    private static final Pattern PATTERN_SOURCE_RECORDS_SIZE = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) HDFS Write");

    // hive
    private static final Pattern PATTERN_HIVE_APP_ID_URL = Pattern.compile("Starting Job = (.*?), Tracking URL = (.*)");
    private static final Pattern PATTERN_HIVE_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) HDFS Write: (\\d+) SUCCESS");

    // spark
    private static final Pattern PATTERN_SPARK_APP_ID = Pattern.compile("Submitted application (.*?)");
    private static final Pattern PATTERN_SPARK_APP_URL = Pattern.compile("tracking URL: (.*)");


    public PatternedLogger(Logger wrappedLogger) {
        super(wrappedLogger);
    }

    @Override
    public void log(String message) {
        super.log(message);
        Matcher matcher = PATTERN_APP_ID.matcher(message);
        if (matcher.find()) {
            String appId = matcher.group(1);
            info.put(ExecutableConstants.YARN_APP_ID, appId);
        }

        matcher = PATTERN_APP_URL.matcher(message);
        if (matcher.find()) {
            String appTrackingUrl = matcher.group(1);
            info.put(ExecutableConstants.YARN_APP_URL, appTrackingUrl);
        }

        matcher = PATTERN_JOB_ID.matcher(message);
        if (matcher.find()) {
            String mrJobID = matcher.group(1);
            info.put(ExecutableConstants.MR_JOB_ID, mrJobID);
        }

        matcher = PATTERN_HDFS_BYTES_WRITTEN.matcher(message);
        if (matcher.find()) {
            String hdfsWritten = matcher.group(1);
            info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, hdfsWritten);
        }

        matcher = PATTERN_SOURCE_RECORDS_COUNT.matcher(message);
        if (matcher.find()) {
            String sourceCount = matcher.group(1);
            info.put(ExecutableConstants.SOURCE_RECORDS_COUNT, sourceCount);
        }

        matcher = PATTERN_SOURCE_RECORDS_SIZE.matcher(message);
        if (matcher.find()) {
            String sourceSize = matcher.group(1);
            info.put(ExecutableConstants.SOURCE_RECORDS_SIZE, sourceSize);
        }

        // hive
        matcher = PATTERN_HIVE_APP_ID_URL.matcher(message);
        if (matcher.find()) {
            String jobId = matcher.group(1);
            String trackingUrl = matcher.group(2);
            info.put(ExecutableConstants.MR_JOB_ID, jobId);
            info.put(ExecutableConstants.YARN_APP_URL, trackingUrl);
        }

        matcher = PATTERN_HIVE_BYTES_WRITTEN.matcher(message);
        if (matcher.find()) {
            // String hdfsRead = matcher.group(1);
            String hdfsWritten = matcher.group(2);
            info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, hdfsWritten);
        }

        // spark
        matcher = PATTERN_SPARK_APP_ID.matcher(message);
        if (matcher.find()) {
            String app_id = matcher.group(1);
            info.put(ExecutableConstants.YARN_APP_ID, app_id);
        }

        matcher = PATTERN_SPARK_APP_URL.matcher(message);
        if (matcher.find()) {
            String trackingUrl = matcher.group(1);
            info.put(ExecutableConstants.YARN_APP_URL, trackingUrl);
        }
    }

    public Map<String, String> getInfo() {
        return info;
    }

}
