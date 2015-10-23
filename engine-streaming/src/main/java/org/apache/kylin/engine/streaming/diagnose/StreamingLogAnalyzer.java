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

package org.apache.kylin.engine.streaming.diagnose;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class StreamingLogAnalyzer {
    public static void main(String[] args) {
        int errorFileCount = 0;
        List<Long> ellapsedTimes = Lists.newArrayList();

        String patternStr = "(\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2})";
        Pattern pattern = Pattern.compile(patternStr);

        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT")); // NOTE: this must be GMT to calculate epoch date correctly

        Preconditions.checkArgument(args.length == 1, "Usage: StreamingLogsAnalyser streaming_logs_folder");
        for (File file : FileUtils.listFiles(new File(args[0]), new String[] { "log" }, false)) {
            System.out.println("Processing file " + file.toString());

            long startTime = 0;
            long endTime = 0;
            try {
                List<String> contents = Files.readAllLines(file.toPath(), Charset.defaultCharset());
                for (int i = 0; i < contents.size(); ++i) {
                    Matcher m = pattern.matcher(contents.get(i));
                    if (m.find()) {
                        startTime = format.parse("20" + m.group(1)).getTime();
                        break;
                    }
                }

                for (int i = contents.size() - 1; i >= 0; --i) {
                    Matcher m = pattern.matcher(contents.get(i));
                    if (m.find()) {
                        endTime = format.parse("20" + m.group(1)).getTime();
                        break;
                    }
                }

                if (startTime == 0 || endTime == 0) {
                    throw new RuntimeException("start time or end time is not found");
                }

                if (endTime - startTime < 60000) {
                    System.out.println("Warning: this job took less than one minute!!!! " + file.toString());
                }

                ellapsedTimes.add(endTime - startTime);

            } catch (Exception e) {
                System.out.println("Exception when processing log file " + file.toString());
                System.out.println(e);
                errorFileCount++;
            }
        }

        System.out.println("Totally error files count " + errorFileCount);
        System.out.println("Totally normal files processed " + ellapsedTimes.size());

        long sum = 0;
        for (Long x : ellapsedTimes) {
            sum += x;
        }
        System.out.println("Avg build time " + (sum / ellapsedTimes.size()));
    }
}
