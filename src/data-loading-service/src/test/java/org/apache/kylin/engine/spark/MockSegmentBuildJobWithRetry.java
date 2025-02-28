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

package org.apache.kylin.engine.spark;

import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.job.SegmentBuildJob;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.spark.dict.IllegalDictEncodeValueException;

public class MockSegmentBuildJobWithRetry extends SegmentBuildJob {

    public static void main(String[] args) {
        System.out.println("start run SegmentBuildJobWithRetryTest");
        SegmentBuildJob segmentBuildJob = new SegmentBuildJob();
        try {
            segmentBuildJob.execute(args);
        } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof IllegalDictEncodeValueException) {
                KylinBuildEnv.get().buildJobInfos()
                        .recordJobRetryInfosForSegmentParam("job.retry.segment.force-build-dict", "true");
                KylinBuildEnv.get().buildJobInfos().recordRetryTimes(1);
                try {
                    segmentBuildJob.execute(args);
                } catch (Exception exception) {
                    throw new RuntimeException("Retry exceeded the maximum limit", exception);
                }
            } else {
                throw new RuntimeException("Not equal to the expected exception", e);
            }
        }
    }

}
