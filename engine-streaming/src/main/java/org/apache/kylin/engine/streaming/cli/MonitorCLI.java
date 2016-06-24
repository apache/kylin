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

package org.apache.kylin.engine.streaming.cli;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.streaming.monitor.StreamingMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class MonitorCLI {

    private static final Logger logger = LoggerFactory.getLogger(MonitorCLI.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args[0].equals("monitor"));

        int i = 1;
        List<String> receivers = null;
        String host = null;
        String tableName = null;
        String authorization = null;
        String cubeName = null;
        String projectName = "default";
        while (i < args.length) {
            String argName = args[i];
            switch (argName) {
            case "-receivers":
                receivers = Lists.newArrayList(StringUtils.split(args[++i], ";"));
                break;
            case "-host":
                host = args[++i];
                break;
            case "-tableName":
                tableName = args[++i];
                break;
            case "-authorization":
                authorization = args[++i];
                break;
            case "-cubeName":
                cubeName = args[++i];
                break;
            case "-projectName":
                projectName = args[++i];
                break;
            default:
                throw new RuntimeException("invalid argName:" + argName);
            }
            i++;
        }
        Preconditions.checkArgument(receivers != null && receivers.size() > 0);
        final StreamingMonitor streamingMonitor = new StreamingMonitor();
        if (tableName != null) {
            logger.info(String.format("check query tableName:%s host:%s receivers:%s", tableName, host, StringUtils.join(receivers, ";")));
            Preconditions.checkNotNull(host);
            Preconditions.checkNotNull(authorization);
            Preconditions.checkNotNull(tableName);
            streamingMonitor.checkCountAll(receivers, host, authorization, projectName, tableName);
        }
        if (cubeName != null) {
            logger.info(String.format("check cube cubeName:%s receivers:%s", cubeName, StringUtils.join(receivers, ";")));
            streamingMonitor.checkCube(receivers, cubeName, host);
        }
        System.exit(0);
    }
}
