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

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.StreamingBatch;
import org.apache.kylin.engine.streaming.util.StreamingUtils;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationType;

import com.google.common.base.Preconditions;

/**
 */
public class OneOffStreamingBuilder {

    private final IStreamingInput streamingInput;
    private final IStreamingOutput streamingOutput;
    private final StreamingBatchBuilder streamingBatchBuilder;
    private final long startTime;
    private final long endTime;
    private final RealizationType realizationType;
    private final String realizationName;

    public OneOffStreamingBuilder(RealizationType realizationType, String realizationName, long startTime, long endTime) {
        Preconditions.checkArgument(startTime < endTime);
        this.startTime = startTime;
        this.endTime = endTime;
        this.realizationType = Preconditions.checkNotNull(realizationType);
        this.realizationName = Preconditions.checkNotNull(realizationName);
        this.streamingInput = Preconditions.checkNotNull(StreamingUtils.getStreamingInput());
        this.streamingOutput = Preconditions.checkNotNull(StreamingUtils.getStreamingOutput());
        this.streamingBatchBuilder = Preconditions.checkNotNull(StreamingUtils.getMicroBatchBuilder(realizationType, realizationName));
    }

    public Runnable build() {
        return new Runnable() {
            @Override
            public void run() {
                StreamingBatch streamingBatch = streamingInput.getBatchWithTimeWindow(realizationType, realizationName, -1, startTime, endTime);
                final IBuildable buildable = streamingBatchBuilder.createBuildable(streamingBatch);
                final Map<Long, HyperLogLogPlusCounter> samplingResult = streamingBatchBuilder.sampling(streamingBatch);
                final Map<TblColRef, Dictionary<String>> dictionaryMap = streamingBatchBuilder.buildDictionary(streamingBatch, buildable);
                streamingBatchBuilder.build(streamingBatch, dictionaryMap, streamingOutput.getCuboidWriter(buildable));
                streamingOutput.output(buildable, samplingResult);
                streamingBatchBuilder.commit(buildable);
            }
        };
    }

}