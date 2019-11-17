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

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.DFBuilderHelper$;
import io.kyligence.kap.engine.spark.builder.DictionaryBuilderHelper;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;
import lombok.var;

public class MockedDFBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(MockedDFBuildJob.class);
    protected volatile NSpanningTree nSpanningTree;

    @Override
    protected void doExecute() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Start Build");
        String dfName = getParam(NBatchConstants.P_DATAFLOW_ID);

        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));

        try {
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            IndexPlan indexPlan = dfMgr.getDataflow(dfName).getIndexPlan();
            Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream()
                    .filter(Objects::nonNull).collect(Collectors.toSet());
            nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dfName);

            for (String segId : segmentIds) {
                NDataSegment seg = dfMgr.getDataflow(dfName).getSegment(segId);
                val dimensions = new ArrayList<Integer>(indexPlan.getModel().getEffectiveCols().keySet());
                List<DataType> sparkTypes = dimensions.stream().map(x -> indexPlan.getModel().getColRef(x).getType())
                        .map(tp -> SparderTypeUtil.toSparkType(tp, false)).collect(Collectors.toList());
                val collect = IntStream.range(0, dimensions.size())
                        .mapToObj(x -> new StructField(String.valueOf(dimensions.get(x)), sparkTypes.get(x), true,
                                Metadata.empty()))
                        .toArray(StructField[]::new);

                var structType = new StructType(collect);
                val needJoin = DFChooser.needJoinLookupTables(seg.getModel(), nSpanningTree);
                val flatTableDesc = new NCubeJoinedFlatTableDesc(indexPlan, seg.getSegRange(), needJoin);
                val nSpanningTree = NSpanningTreeFactory.fromLayouts(indexPlan.getAllLayouts(), dfName);
                for (TblColRef ref : DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, nSpanningTree)) {
                    int columnIndex = flatTableDesc.getColumnIndex(ref);
                    structType = structType.add(
                            structType.apply(columnIndex).name() + DFBuilderHelper$.MODULE$.ENCODE_SUFFIX(),
                            IntegerType);
                }

                Dataset<Row> ds = ss.createDataFrame(Lists.newArrayList(), structType);

                cuboids.forEach(layout -> {
                    CuboidAggregator.agg(ss, ds, layout.getOrderedDimensions().keySet(),
                            indexPlan.getEffectiveMeasures(), seg, nSpanningTree);

                    NDataLayout dataCuboid = NDataLayout.newDataLayout(seg.getDataflow(), seg.getId(), layout.getId());
                    dataCuboid.setRows(123);
                    dataCuboid.setSourceByteSize(123);
                    dataCuboid.setSourceRows(123);
                    dataCuboid.setBuildJobId(UUID.randomUUID().toString());
                    dataCuboid.setFileCount(123);
                    dataCuboid.setByteSize(123);
                    StorageFactory.createEngineAdapter(layout, NSparkCubingEngine.NSparkCubingStorage.class)
                            .saveTo(NSparkCubingUtil.getStoragePath(dataCuboid), ds, ss);

                    NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getUuid());
                    update.setToAddOrUpdateLayouts(dataCuboid);
                    NDataflowManager.getInstance(config, project).updateDataflow(update);

                });
            }

        } finally {
            logger.info("Finish build take" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    public static void main(String[] args) {
        MockedDFBuildJob nDataflowBuildJob = new MockedDFBuildJob();
        nDataflowBuildJob.execute(args);
    }

}
