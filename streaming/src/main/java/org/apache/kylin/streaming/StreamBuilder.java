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
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 */
public abstract class StreamBuilder implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

    private StreamParser streamParser = StringStreamParser.instance;

    private StreamFilter streamFilter = DefaultStreamFilter.instance;

    private BlockingQueue<StreamMessage> streamMessageQueue;
    private long lastBuildTime = System.currentTimeMillis();

    private long startOffset;
    private long endOffset;

    private long startTimestamp;
    private long endTimestamp;

    public StreamBuilder(BlockingQueue<StreamMessage> streamMessageQueue) {
        this.streamMessageQueue = streamMessageQueue;
    }

    protected abstract void build(MicroStreamBatch microStreamBatch) throws Exception;

    protected abstract void onStop();

    private void clearCounter() {
        lastBuildTime = System.currentTimeMillis();
        startOffset = Long.MAX_VALUE;
        endOffset = Long.MIN_VALUE;
        startTimestamp = Long.MAX_VALUE;
        endTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void run() {
        try {
            List<List<String>> parsedStreamMessages = null;
            int filteredMsgCount = 0;
            while (true) {
                if (parsedStreamMessages == null) {
                    parsedStreamMessages = Lists.newLinkedList();
                    clearCounter();
                }
                StreamMessage streamMessage;
                try {
                    streamMessage = streamMessageQueue.poll(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.warn("stream queue should not be interrupted", e);
                    continue;
                }
                if (streamMessage == null) {
                    logger.info("The stream queue is drained, current available stream count: " + parsedStreamMessages.size());
                    if ((System.currentTimeMillis() - lastBuildTime) > batchInterval() && !parsedStreamMessages.isEmpty()) {
                        logger.info("Building batch due to time threshold, batch size: " + parsedStreamMessages.size());
                        build(new MicroStreamBatch(parsedStreamMessages, Pair.newPair(startTimestamp, endTimestamp), Pair.newPair(startOffset, endOffset)));
                        parsedStreamMessages = null;
                    }
                    continue;
                }
                if (streamMessage.getOffset() < 0) {
                    onStop();
                    logger.warn("streaming encountered EOF, stop building. The remaining {} filtered messages will be discarded", filteredMsgCount);
                    break;
                }

                final ParsedStreamMessage parsedStreamMessage = getStreamParser().parse(streamMessage);

                if (getStreamFilter().filter(parsedStreamMessage)) {

                    if (filteredMsgCount++ % 10000 == 0) {
                        logger.info("Total filtered stream message count: " + filteredMsgCount);
                    }

                    if (startOffset > parsedStreamMessage.getOffset()) {
                        startOffset = parsedStreamMessage.getOffset();
                    }
                    if (endOffset < parsedStreamMessage.getOffset()) {
                        endOffset = parsedStreamMessage.getOffset();
                    }
                    if (startTimestamp > parsedStreamMessage.getTimestamp()) {
                        startTimestamp = parsedStreamMessage.getTimestamp();
                    }
                    if (endTimestamp < parsedStreamMessage.getTimestamp()) {
                        endTimestamp = parsedStreamMessage.getTimestamp();
                    }
                    parsedStreamMessages.add(parsedStreamMessage.getStreamMessage());
                    if (parsedStreamMessages.size() >= batchSize()) {
                        logger.info("Building batch due to size threshold, batch size: " + parsedStreamMessages.size());
                        build(new MicroStreamBatch(parsedStreamMessages, Pair.newPair(startTimestamp, endTimestamp), Pair.newPair(startOffset, endOffset)));
                        parsedStreamMessages = null;
                    }
                } else {
                    //ignore unfiltered stream message
                }

            }
        } catch (Exception e) {
            logger.error("build stream error, stop building", e);
            throw new RuntimeException("build stream error, stop building", e);
        }
    }

    public final StreamParser getStreamParser() {
        return streamParser;
    }

    public final void setStreamParser(StreamParser streamParser) {
        this.streamParser = streamParser;
    }

    public final StreamFilter getStreamFilter() {
        return streamFilter;
    }

    public final void setStreamFilter(StreamFilter streamFilter) {
        this.streamFilter = streamFilter;
    }

    protected abstract int batchInterval();

    protected abstract int batchSize();
}
