/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.job.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qianzhou on 3/26/15.
 */
public class StreamingCLI {

    private static final Logger logger = LoggerFactory.getLogger(StreamingCLI.class);

    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                printArgsError(args);
                return;
            }
            if (args[0].equals("start")) {
                String kafkaConfName = args[1];
                int partition = Integer.parseInt(args[2]);
                StreamingBootstrap.getInstance(KylinConfig.getInstanceFromEnv()).start(kafkaConfName, partition);
            } else {
                printArgsError(args);
            }
        } catch (Exception e) {
            printArgsError(args);
            logger.error("error start streaming", e);
            System.exit(-1);
        }
    }

    private static void printArgsError(String[] args) {
        logger.warn("invalid args:" + StringUtils.join(args, " "));
    }

}
