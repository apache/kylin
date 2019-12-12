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

package io.kyligence.kap.engine.spark.job;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
public class NSparkMergingStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    public NSparkMergingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return df.collectPrecalculationResource();
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(NSparkMergingStep.Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
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

        val lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            val path = Paths.get(entry.getValue()).getParent().toString();
            result.add("/" + path);
        }

        val allSegments = Lists.newArrayList(mergingSegments);
        allSegments.add(mergedSeg);
        for (NDataSegment seg : allSegments) {
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                val dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layout.getId());
                String path = "/" + NSparkCubingUtil.getStoragePathWithoutPrefix(dataCuboid.getSegDetails(),
                        dataCuboid.getLayoutId());
                result.add(path);
                result.add(path + CubeBuildJob.TEMP_DIR_SUFFIX);
            }
        }

        return result;

    }
}
