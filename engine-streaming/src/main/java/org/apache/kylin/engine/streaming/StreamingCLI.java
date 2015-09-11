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

package org.apache.kylin.engine.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.cache.RemoteCacheUpdater;
import org.apache.kylin.common.restclient.AbstractRestCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class StreamingCLI {

    private static final Logger logger = LoggerFactory.getLogger(StreamingCLI.class);

    public static void main(String[] args) {
        try {
            AbstractRestCache.setCacheUpdater(new RemoteCacheUpdater());

            Preconditions.checkArgument(args[0].equals("streaming"));
            Preconditions.checkArgument(args[1].equals("start"));

            int i = 2;
            BootstrapConfig bootstrapConfig = new BootstrapConfig();
            while (i < args.length) {
                String argName = args[i];
                switch (argName) {
                case "-oneoff":
                    bootstrapConfig.setOneOff(Boolean.parseBoolean(args[++i]));
                    break;
                case "-start":
                    bootstrapConfig.setStart(Long.parseLong(args[++i]));
                    break;
                case "-end":
                    bootstrapConfig.setEnd(Long.parseLong(args[++i]));
                    break;
                case "-streaming":
                    bootstrapConfig.setStreaming(args[++i]);
                    break;
                case "-partition":
                    bootstrapConfig.setPartitionId(Integer.parseInt(args[++i]));
                    break;
                case "-fillGap":
                    bootstrapConfig.setFillGap(Boolean.parseBoolean(args[++i]));
                    break;
                default:
                    logger.warn("ignore this arg:" + argName);
                }
                i++;
            }
            final Runnable runnable = new OneOffStreamingBuilder(bootstrapConfig.getStreaming(), bootstrapConfig.getStart(), bootstrapConfig.getEnd()).build();
            runnable.run();
            logger.info("streaming process stop, exit with 0");
            System.exit(0);
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
