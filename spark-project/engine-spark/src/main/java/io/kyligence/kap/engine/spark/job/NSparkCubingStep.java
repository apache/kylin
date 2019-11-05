/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Arrays;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 */
@NoArgsConstructor

public class NSparkCubingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    public NSparkCubingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return df.collectPrecalculationResource();
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
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
        Set<String> segmentIds = getSegmentIds();

        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val dataflow = dfMgr.getDataflow(dataflowId);
        val indexPlan = dataflow.getIndexPlan();

        Set<String> result = Sets.newHashSet();
        for (String segId : segmentIds) {
            val seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            for (LayoutEntity layout : indexPlan.getAllLayouts()) {
                NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layout.getId());
                String path = "/" + NSparkCubingUtil.getStoragePathWithoutPrefix(dataCuboid.getSegDetails(),
                        dataCuboid.getLayoutId());
                result.add(path);
                result.add(path + DFBuildJob.TEMP_DIR_SUFFIX);
            }
        }

        val model = indexPlan.getModel();
        model.getJoinTables().forEach(lookupDesc -> {
            val tableDesc = lookupDesc.getTableRef().getTableDesc();
            val isLookupTable = model.isLookupTable(lookupDesc.getTableRef());
            if (isLookupTable) {
                val tablePath = "/" + tableDesc.getProject() + HadoopUtil.SNAPSHOT_STORAGE_ROOT + "/"
                        + tableDesc.getName();
                result.add(tablePath);
            }
        });
        return result;
    }
}
