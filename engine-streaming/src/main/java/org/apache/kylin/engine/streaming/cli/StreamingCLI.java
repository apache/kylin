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

package org.apache.kylin.engine.streaming.cli;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.streaming.BootstrapConfig;
import org.apache.kylin.engine.streaming.OneOffStreamingBuilder;
import org.apache.kylin.engine.streaming.monitor.StreamingMonitor;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class StreamingCLI {

    private static final Logger logger = LoggerFactory.getLogger(StreamingCLI.class);

    public static void main(String[] args) {
        try {
            Preconditions.checkArgument(args[0].equals("streaming"));
            Preconditions.checkArgument(args[1].equals("start"));

            int i = 2;
            BootstrapConfig bootstrapConfig = new BootstrapConfig();
            while (i < args.length) {
                String argName = args[i];
                switch (argName) {
                case "-start":
                    bootstrapConfig.setStart(Long.parseLong(args[++i]));
                    break;
                case "-end":
                    bootstrapConfig.setEnd(Long.parseLong(args[++i]));
                    break;
                case "-cube":
                    bootstrapConfig.setCubeName(args[++i]);
                    break;
                case "-fillGap":
                    bootstrapConfig.setFillGap(Boolean.parseBoolean(args[++i]));
                    break;
                case "-maxFillGapRange":
                    bootstrapConfig.setMaxFillGapRange(Long.parseLong(args[++i]));
                    break;
                default:
                    logger.warn("ignore this arg:" + argName);
                }
                i++;
            }
            if (bootstrapConfig.isFillGap()) {
                final List<Pair<Long, Long>> gaps = StreamingMonitor.findGaps(bootstrapConfig.getCubeName());
                logger.info("all gaps:" + StringUtils.join(gaps, ","));
                for (Pair<Long, Long> gap : gaps) {
                    List<Pair<Long, Long>> splitGaps = splitGap(gap, bootstrapConfig.getMaxFillGapRange());
                    for (Pair<Long, Long> splitGap : splitGaps) {
                        logger.info("start filling the gap from " + splitGap.getFirst() + " to " + splitGap.getSecond());
                        startOneOffCubeStreaming(bootstrapConfig.getCubeName(), splitGap.getFirst(), splitGap.getSecond());
                        logger.info("finish filling the gap from " + splitGap.getFirst() + " to " + splitGap.getSecond());
                    }
                }
            } else {
                startOneOffCubeStreaming(bootstrapConfig.getCubeName(), bootstrapConfig.getStart(), bootstrapConfig.getEnd());
                logger.info("streaming process finished, exit with 0");
                System.exit(0);
            }
        } catch (Exception e) {
            printArgsError(args);
            logger.error("error start streaming", e);
            System.exit(-1);
        }
    }

    private static List<Pair<Long, Long>> splitGap(Pair<Long, Long> gap, long maxFillGapRange) {
        List<Pair<Long, Long>> gaps = Lists.newArrayList();
        Long startTime = gap.getFirst();

        while (startTime < gap.getSecond()) {
            Long endTime = gap.getSecond() <= startTime + maxFillGapRange ? gap.getSecond() : startTime + maxFillGapRange;
            gaps.add(Pair.newPair(startTime, endTime));
            startTime = endTime;
        }

        return gaps;
    }

    private static void startOneOffCubeStreaming(String cubeName, long start, long end) {
        final Runnable runnable = new OneOffStreamingBuilder(RealizationType.CUBE, cubeName, start, end).build();
        runnable.run();
    }

    private static void printArgsError(String[] args) {
        logger.warn("invalid args:" + StringUtils.join(args, " "));
    }

}
