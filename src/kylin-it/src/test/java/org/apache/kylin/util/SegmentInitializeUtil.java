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


package org.apache.kylin.util;

import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;

import lombok.val;

public class SegmentInitializeUtil {

    public static void prepareSegment(KylinConfig config, String project, String dfId, String start, String end,
            boolean needRemoveExist) {
        val jobManager = JobManager.getInstance(config, project);
        val dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow df = dataflowManager.getDataflow(dfId);
        // remove the existed seg
        if (needRemoveExist) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            df = dataflowManager.updateDataflow(update);
        }
        val newSeg = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)));

        // add first segment
        val jobId = jobManager.addSegmentJob(new JobParam(newSeg, df.getModel().getUuid(), "ADMIN"));
        JobFinishHelper.waitJobFinish(config, project, jobId, 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap2 = df2.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df2.getIndexPlan().getAllLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }
}
