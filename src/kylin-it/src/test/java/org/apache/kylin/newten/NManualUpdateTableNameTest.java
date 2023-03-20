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

package org.apache.kylin.newten;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.builder.CreateFlatTable;
import org.apache.kylin.engine.spark.job.CuboidAggregator;
import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapSerializer;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NCubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.service.update.TableSchemaUpdateMapping;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.common.SparderQueryTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import org.springframework.test.util.ReflectionTestUtils;

public class NManualUpdateTableNameTest extends NManualBuildAndQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    private static final String DEFAULT_PROJECT = "default";

    private static StructType OUT_SCHEMA = null;

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    private final String mappingRootPath = "../common-service/src/test/resources/update";
    private final String mappingFileName = "TableSchemaUpdateMapping.json";
    private Map<String, TableSchemaUpdateMapping> mappings;

    @Before
    public void setup() throws Exception {
        super.init();
        overwriteSystemProp("spark.local", "true");
        overwriteSystemProp("noBuild", "false");
        overwriteSystemProp("isDeveloperMode", "false");

        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));

        File mappingFile = new File(mappingRootPath + "/" + mappingFileName);
        String content = new String(Files.readAllBytes(mappingFile.toPath()), StandardCharsets.UTF_8);
        mappings = JsonUtil.readValue(content, new TypeReference<Map<String, TableSchemaUpdateMapping>>() {
        });
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return DEFAULT_PROJECT;
    }

    @Test
    public void testBasics() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.support-update-computed-column-mapping", "true");

        // build target models
        buildCubes();
        List<String> onlineModels = new ArrayList<>();
        onlineModels.add("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        onlineModels.add("741ca86a-1f13-46da-a59f-95fb68615e3a");

        // test query before updating hive table names
        for (String modelId : onlineModels) {
            compareCuboidParquetWithSparkSql(modelId);
        }

        // offline target model
        for (String modelId : onlineModels) {
            modelService.updateDataModelStatus(modelId, DEFAULT_PROJECT, "OFFLINE");
        }

        // update hive table name
        tableService.updateHiveTable(DEFAULT_PROJECT, mappings, null, false);

        // online target model
        for (String modelId : onlineModels) {
            modelService.updateDataModelStatus(modelId, DEFAULT_PROJECT, "ONLINE");
        }

        // test query after updating hive table names
        for (String modelId : onlineModels) {
            compareCuboidParquetWithSparkSql(modelId);
        }
    }

    private void compareCuboidParquetWithSparkSql(String dfName) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        List<NDataLayout> dataLayouts = Lists.newArrayList();
        NDataflow df = dsMgr.getDataflow(dfName);
        for (NDataSegment segment : df.getSegments()) {
            dataLayouts.addAll(segment.getSegDetails().getLayouts());
        }
        for (NDataLayout cuboid : dataLayouts) {
            Set<Integer> rowKeys = cuboid.getLayout().getOrderedDimensions().keySet();

            Dataset<Row> layoutDataset = StorageFactory
                    .createEngineAdapter(cuboid.getLayout(), NSparkCubingEngine.NSparkCubingStorage.class)
                    .getFrom(NSparkCubingUtil.getStoragePath(cuboid.getSegDetails().getDataSegment(),
                            cuboid.getLayoutId()), ss);
            layoutDataset = layoutDataset.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid)))
                    .sort(NSparkCubingUtil.getColumns(rowKeys));
            logger.debug("Query cuboid ------------ " + cuboid.getLayoutId());
            layoutDataset = dsConvertToOriginal(layoutDataset, cuboid.getLayout());
            logger.debug(layoutDataset.showString(10, 20, false));

            NDataSegment segment = cuboid.getSegDetails().getDataSegment();
            Dataset<Row> ds = initFlatTable(dfName, new SegmentRange.TimePartitionedSegmentRange(
                    segment.getTSRange().getStart(), segment.getTSRange().getEnd()));

            if (cuboid.getLayout().getIndex().getId() < IndexEntity.TABLE_INDEX_START_ID) {
                ds = queryCuboidLayout(cuboid.getLayout(), ds);
            }

            Dataset<Row> exceptDs = ds.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid)))
                    .sort(NSparkCubingUtil.getColumns(rowKeys));

            logger.debug("Spark sql ------------ ");
            logger.debug(exceptDs.showString(10, 20, false));

            Assert.assertEquals(layoutDataset.count(), exceptDs.count());
            String msg = SparderQueryTest.checkAnswer(layoutDataset, exceptDs, false);
            Assert.assertNull(msg);
        }
    }

    private Set<Integer> chooseMeas(NDataLayout cuboid) {
        Set<Integer> meaSet = Sets.newHashSet();
        for (Map.Entry<Integer, NDataModel.Measure> entry : cuboid.getLayout().getOrderedMeasures().entrySet()) {
            String funName = entry.getValue().getFunction().getReturnDataType().getName();
            if (funName.equals("hllc") || funName.equals("topn") || funName.equals("percentile")) {
                continue;
            }
            meaSet.add(entry.getKey());
        }
        return meaSet;
    }

    private Dataset<Row> queryCuboidLayout(LayoutEntity layout, Dataset<Row> ds) {
        NCubeJoinedFlatTableDesc tableDesc = new NCubeJoinedFlatTableDesc(layout.getIndex().getIndexPlan());
        return CuboidAggregator.aggregateJava(ds, layout.getIndex().getEffectiveDimCols().keySet(), //
                layout.getIndex().getIndexPlan().getEffectiveMeasures(), //
                tableDesc, true);
    }

    private Dataset<Row> dsConvertToOriginal(Dataset<Row> layoutDs, LayoutEntity layout) {
        ImmutableBiMap<Integer, NDataModel.Measure> orderedMeasures = layout.getOrderedMeasures();

        for (final Map.Entry<Integer, NDataModel.Measure> entry : orderedMeasures.entrySet()) {
            MeasureDesc measureDesc = entry.getValue();
            if (measureDesc != null) {
                final String[] columns = layoutDs.columns();
                String function = measureDesc.getFunction().getReturnDataType().getName();

                if ("bitmap".equals(function)) {
                    final int finalIndex = convertOutSchema(layoutDs, entry.getKey().toString(), DataTypes.LongType);
                    layoutDs = layoutDs.map((MapFunction<Row, Row>) value -> {
                        Object[] ret = new Object[value.size()];
                        for (int i = 0; i < columns.length; i++) {
                            if (i == finalIndex) {
                                BitmapSerializer serializer = new BitmapSerializer(DataType.ANY);
                                byte[] bytes = (byte[]) value.get(i);
                                ByteBuffer buf = ByteBuffer.wrap(bytes);
                                BitmapCounter bitmapCounter = serializer.deserialize(buf);
                                ret[i] = bitmapCounter.getCount();
                            } else {
                                ret[i] = value.get(i);
                            }
                        }
                        return RowFactory.create(ret);
                    }, RowEncoder.apply(OUT_SCHEMA));
                }
            }
        }
        return layoutDs;
    }

    private Integer convertOutSchema(Dataset<Row> layoutDs, String fieldName,
            org.apache.spark.sql.types.DataType dataType) {
        StructField[] structFieldList = layoutDs.schema().fields();
        String[] columns = layoutDs.columns();

        int index = 0;
        StructField[] outStructFieldList = new StructField[structFieldList.length];
        for (int i = 0; i < structFieldList.length; i++) {
            if (columns[i].equalsIgnoreCase(fieldName)) {
                index = i;
                StructField structField = structFieldList[i];
                outStructFieldList[i] = new StructField(structField.name(), dataType, false, structField.metadata());
            } else {
                outStructFieldList[i] = structFieldList[i];
            }
        }

        OUT_SCHEMA = new StructType(outStructFieldList);

        return index;
    }

    private Dataset<Row> initFlatTable(String dfName, SegmentRange segmentRange) {
        System.out.println(getTestConfig().getMetadataUrl());
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataModel model = df.getModel();

        NCubeJoinedFlatTableDesc flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan(), segmentRange, true);
        CreateFlatTable flatTable = new CreateFlatTable(flatTableDesc, null, null, ss, null);
        Dataset<Row> ds = flatTable.generateDataset(false, true);

        StructType schema = ds.schema();
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(model.getColumnNameByColumnId(Integer.parseInt(field.name()))));
        }
        return ds;
    }
}
