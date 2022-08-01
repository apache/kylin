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

package org.apache.kylin.engine.spark.job;

import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.engine.spark.merger.MetadataMerger;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public class NSparkMergingStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingStep.class);

    public NSparkMergingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT);
    }

    public NSparkMergingStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return df.collectPrecalculationResource();
    }

    @Override
    public void mergerMetadata(MetadataMerger merger) {
        merger.merge(this);
    }

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

    @Override
    public Set<String> getDependencies(KylinConfig config) {
        String dataflowId = getDataflowId();
        String segmentId = getParam(NBatchConstants.P_SEGMENT_IDS);

        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val dataflow = dfMgr.getDataflow(dataflowId);
        val indexPlan = dataflow.getIndexPlan();
        val mergedSeg = dataflow.getSegment(segmentId);
        val mergingSegments = dataflow.getMergingSegments(mergedSeg);

        Set<String> result = Sets.newHashSet();

        val allSegments = Lists.newArrayList(mergingSegments);
        allSegments.add(mergedSeg);
        for (NDataSegment seg : allSegments) {
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                String path = "/" + NSparkCubingUtil.getStoragePathWithoutPrefix(project, dataflowId, seg.getId(),
                        layout.getId());
                result.add(new Path(path).getParent().toString());
            }
        }

        return result;

    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(NSparkMergingStep.Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }
}
