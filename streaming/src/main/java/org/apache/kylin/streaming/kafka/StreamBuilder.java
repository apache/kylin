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

package org.apache.kylin.streaming.kafka;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by qianzhou on 2/17/15.
 */
public abstract class StreamBuilder implements Runnable {

    private List<BlockingQueue<Stream>> streamQueues;
    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);
    private final int batchBuildCount;

    public StreamBuilder(List<BlockingQueue<Stream>> streamQueues, int batchBuildCount) {
        this.streamQueues = streamQueues;
        this.batchBuildCount = batchBuildCount;
    }


    private int getEarliestStreamIndex(Stream[] streamHead) {
        long ts = Long.MAX_VALUE;
        int idx = 0;
        for (int i = 0; i < streamHead.length; i++) {
            if (streamHead[i].getTimestamp() < ts) {
                ts = streamHead[i].getTimestamp();
                idx = i;
            }
        }
        return idx;
    }

    protected abstract void build(List<Stream> streamsToBuild);

    @Override
    public void run() {
        try {
            Stream[] streamHead = new Stream[streamQueues.size()];
            for (int i = 0; i < streamQueues.size(); i++) {
                streamHead[i] = streamQueues.get(i).take();
            }
            List<Stream> streamToBuild = Lists.newArrayListWithCapacity(batchBuildCount);
            while (true) {
                if (streamToBuild.size() >= batchBuildCount) {
                    build(streamToBuild);
                    streamToBuild.clear();
                }
                int idx = getEarliestStreamIndex(streamHead);
                streamToBuild.add(streamHead[idx]);
                streamHead[idx] = streamQueues.get(idx).take();
            }
        } catch (InterruptedException e) {
            logger.error("", e);
        }
    }
}
