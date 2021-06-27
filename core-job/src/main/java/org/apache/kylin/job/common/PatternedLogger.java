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
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * A logger which parses certain patterns from log
 */
public class PatternedLogger extends BufferedLogger {
    private final Map<String, String> info = Maps.newHashMap();
    private ILogListener listener = null;

    private static final Pattern PATTERN_APP_ID = Pattern.compile("Submitted application (.*?) to ResourceManager");
    private static final Pattern PATTERN_APP_URL = Pattern.compile("The url to track the job: (.*)");
    private static final Pattern PATTERN_JOB_ID = Pattern.compile("Running job: (.*)");
    private static final Pattern PATTERN_HDFS_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS: Number of bytes written=(\\d+)");
    private static final Pattern PATTERN_SOURCE_RECORDS_COUNT = Pattern.compile("Map input records=(\\d+)");
    private static final Pattern PATTERN_SOURCE_RECORDS_SIZE = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) (?:HD|MAPR)FS Write");

    // hive
    private static final Pattern PATTERN_HIVE_APP_ID_URL = Pattern.compile("Starting Job = (.*?), Tracking URL = (.*)");
    private static final Pattern PATTERN_HIVE_BYTES_WRITTEN = Pattern.compile("(?:HD|MAPR)FS Read: (\\d+) (?:HD|MAPR)FS Write: (\\d+) SUCCESS");

    private static final Pattern PATTERN_HIVE_APP_ID_URL_2 = Pattern.compile("Executing on YARN cluster with App id  (.*?)");

    // spark
    private static final Pattern PATTERN_SPARK_APP_ID = Pattern.compile("Submitted application (.*)");
    private static final Pattern PATTERN_SPARK_APP_URL = Pattern.compile("(?i)Tracking URL: (.*)");
    private static final Pattern PATTERN_JOB_STATE = Pattern.compile("Final-State : (.*?)$");

    //flink
    private static final Pattern PATTERN_FLINK_APP_ID = Pattern.compile("Submitted application (.*)");
    private static final Pattern PATTERN_FLINK_APP_URL = Pattern.compile("tracking URL: (.*)");


    private static Map<Pattern, Pair<String, Integer>> patternMap = Maps.newHashMap(); // key is pattern, value is a pair, the first is property key, second is pattern index.

    static {
        patternMap.put(PATTERN_APP_ID, new Pair(ExecutableConstants.YARN_APP_ID, 1));
        patternMap.put(PATTERN_APP_URL, new Pair(ExecutableConstants.YARN_APP_URL, 1));
        patternMap.put(PATTERN_JOB_ID, new Pair(ExecutableConstants.MR_JOB_ID, 1));
        patternMap.put(PATTERN_HDFS_BYTES_WRITTEN, new Pair(ExecutableConstants.HDFS_BYTES_WRITTEN, 1));
        patternMap.put(PATTERN_SOURCE_RECORDS_COUNT, new Pair(ExecutableConstants.SOURCE_RECORDS_COUNT, 1));
        patternMap.put(PATTERN_SOURCE_RECORDS_SIZE, new Pair(ExecutableConstants.SOURCE_RECORDS_SIZE, 1));
        patternMap.put(PATTERN_HIVE_APP_ID_URL, new Pair(ExecutableConstants.YARN_APP_URL, 2));
        patternMap.put(PATTERN_HIVE_APP_ID_URL_2, new Pair(ExecutableConstants.YARN_APP_URL, 1));
        patternMap.put(PATTERN_HIVE_BYTES_WRITTEN, new Pair(ExecutableConstants.HDFS_BYTES_WRITTEN, 2));
        patternMap.put(PATTERN_SPARK_APP_ID, new Pair(ExecutableConstants.SPARK_JOB_ID, 1));
        patternMap.put(PATTERN_SPARK_APP_URL, new Pair(ExecutableConstants.YARN_APP_URL, 1));
        patternMap.put(PATTERN_JOB_STATE, new Pair(ExecutableConstants.YARN_APP_STATE, 1));
        patternMap.put(PATTERN_FLINK_APP_ID, new Pair(ExecutableConstants.FLINK_JOB_ID, 1));
        patternMap.put(PATTERN_FLINK_APP_URL, new Pair(ExecutableConstants.YARN_APP_URL, 1));
    }

    public PatternedLogger(Logger wrappedLogger) {
        super(wrappedLogger);
    }

    public PatternedLogger(Logger wrappedLogger, ILogListener listener) {
        super(wrappedLogger);
        this.listener = listener;
    }

    @Override
    public void log(String message) {
        super.log(message);
        Matcher matcher;
        for (Pattern pattern : patternMap.keySet()) {
            matcher = pattern.matcher(message);
            if (matcher.find()) {
                String key = patternMap.get(pattern).getFirst();
                int index = patternMap.get(pattern).getSecond();
                String value = matcher.group(index);
                info.put(key, value);
                if (listener != null) {
                    listener.onLogEvent(key, info);
                }
                break;
            }
        }

    }

    public Map<String, String> getInfo() {
        return info;
    }

    // Listener interface on notify pattern matched event
    public interface ILogListener{
        void onLogEvent(String infoKey, Map<String, String> info);
    }

    public void setILogListener(ILogListener listener){
        this.listener = listener;
    }
}
