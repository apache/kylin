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

package org.apache.kylin.streaming;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by qianzhou on 2/17/15.
 */
public abstract class StreamBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

    private static final int BATCH_BUILD_BYTES_THRESHOLD = 64 * 1024;
    private static final int BATCH_BUILD_INTERVAL_THRESHOLD = 5 * 60 * 1000;

    private BlockingQueue<Stream> streamQueue;
    private long lastBuildTime = System.currentTimeMillis();
    private int bytesTotal = 0;

    public StreamBuilder(BlockingQueue<Stream> streamQueue) {
        this.streamQueue = streamQueue;
    }

    protected abstract void build(List<Stream> streamsToBuild);

    private void buildStream(List<Stream> streams) {
        build(streams);
        clearCounter();
    }

    private void clearCounter() {
        lastBuildTime = System.currentTimeMillis();
        bytesTotal = 0;
    }

    @Override
    public void run() {
        try {
            List<Stream> streamToBuild = Lists.newArrayList();
            clearCounter();
            while (true) {
                final Stream stream = streamQueue.take();
                streamToBuild.add(stream);
                bytesTotal += stream.getRawData().length;
                if (bytesTotal >= BATCH_BUILD_BYTES_THRESHOLD) {
                    buildStream(streamToBuild);
                } else if ((System.currentTimeMillis() - lastBuildTime) > BATCH_BUILD_INTERVAL_THRESHOLD) {
                    buildStream(streamToBuild);
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.error("StreamBuilder has been interrupted", e);
        }
    }
}
