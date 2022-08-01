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
package org.apache.kylin.streaming.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StreamingMergeApplication extends StreamingApplication {
    private final Map<String, Pair<String, Long>> removeSegIds = new HashMap<>();
    @Getter
    @Setter
    protected long thresholdOfSegSize;
    @Getter
    @Setter
    protected Integer numberOfSeg;

    protected StreamingMergeApplication() {
        this.jobType = JobTypeEnum.STREAMING_MERGE;
    }

    public void parseParams(String[] args) {
        this.project = args[0];
        this.dataflowId = args[1];
        this.thresholdOfSegSize = StreamingUtils.parseSize(args[2]);
        this.numberOfSeg = Integer.parseInt(args[3]);
        this.distMetaUrl = args[4];
        this.jobId = StreamingUtils.getJobId(dataflowId, jobType.name());

        Preconditions.checkArgument(StringUtils.isNotEmpty(distMetaUrl), "distMetaUrl should not be empty!");
    }

    public void putHdfsFile(String segId, Pair<String, Long> item) {
        removeSegIds.put(segId, item);
    }

    public void clearHdfsFiles(NDataflow dataflow, AtomicLong startTime) {
        val hdfsFileScanStartTime = startTime.get();
        long now = System.currentTimeMillis();
        val intervals = KylinConfig.getInstanceFromEnv().getStreamingSegmentCleanInterval() * 60 * 60 * 1000;
        if (now - hdfsFileScanStartTime > intervals) {
            val iter = removeSegIds.keySet().iterator();
            while (iter.hasNext()) {
                String segId = iter.next();
                if (dataflow.getSegment(segId) != null) {
                    continue;
                }
                if ((now - removeSegIds.get(segId).getValue()) > intervals * 10) {
                    iter.remove();
                } else if ((now - removeSegIds.get(segId).getValue()) > intervals) {
                    try {
                        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                                new Path(removeSegIds.get(segId).getKey()));
                        iter.remove();
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                }
            }
            startTime.set(now);
        }
    }

}
