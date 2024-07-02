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

package org.apache.kylin.engine.spark.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.job.InternalTableLoadingJob;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NSparkCubingStep;
import org.apache.kylin.engine.spark.job.NSparkIndexPlanOptimizeJob;
import org.apache.kylin.engine.spark.job.NSparkLayoutDataOptimizeJob;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

public class SparkJobFactoryUtils {

    private SparkJobFactoryUtils() {
    }

    public static void initJobFactory() {
        // register jobFactory in static function
        new NSparkCubingJob();
        new NSparkMergingJob();
        new NSparkSnapshotJob();
        new NTableSamplingJob();
        new NSparkLayoutDataOptimizeJob();
        new InternalTableLoadingJob();
        new NSparkIndexPlanOptimizeJob();
    }

    public static boolean needBuildSnapshots(AbstractExecutable buildTask) {
        if (buildTask instanceof NSparkCubingStep) {
            String p = buildTask.getParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS);
            return StringUtils.isBlank(p) || Boolean.parseBoolean(p);
        } else {
            return false;
        }
    }
}
