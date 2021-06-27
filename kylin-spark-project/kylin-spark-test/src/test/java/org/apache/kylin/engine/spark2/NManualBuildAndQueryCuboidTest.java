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
package org.apache.kylin.engine.spark2;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.job.CuboidAggregator;
import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.kylin.engine.spark.metadata.FunctionDesc;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.common.SparkQueryTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.udaf.PreciseCountDistinct;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class NManualBuildAndQueryCuboidTest extends NBuildAndQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryCuboidTest.class);
    private static final String DEFAULT_PROJECT = "default";
    private static StructType OUT_SCHEMA = null;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        System.setProperty("noBuild", "false");
        System.setProperty("isDeveloperMode", "false");
    }

    @After
    public void after() {
        System.clearProperty("noBuild");
        System.clearProperty("isDeveloperMode");
        super.after();
    }

    @Override
    public String getProject() {
        return DEFAULT_PROJECT;
    }

    @Test
    public void testBasics() throws Exception {
        buildCubes();
        compareCuboidParquetWithSparkSql("ci_left_join_cube");
    }

    /**
     * avoid to run the test case in parent class 'NBuildAndQueryTest'
     */
    @Override
    public void verifySqlStandard() throws Exception {}

    private void compareCuboidParquetWithSparkSql(String cubeName) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        CubeManager cubeMgr = CubeManager.getInstance(config);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        CubeInstance cube = cubeMgr.getCube(cubeName);
        for (CubeSegment segment : cube.getSegments()) {
            List<LayoutEntity> dataLayouts = MetadataConverter.extractEntityList2JavaList(segment.getCubeInstance());

            for (LayoutEntity cuboid : dataLayouts) {
                Set<Integer> rowKeys = cuboid.getOrderedDimensions().keySet();

                Dataset<Row> layoutDataset = StorageFactory
                        .createEngineAdapter(new IStorageAware() { // Hardcode
                            @Override
                            public int getStorageType() {
                                return 4;
                            }
                        }, NSparkCubingEngine.NSparkCubingStorage.class)
                        .getFrom(PathManager.getParquetStoragePath(segment.getConfig(),
                                segment.getCubeInstance().getName(),
                                segment.getName(), segment.getStorageLocationIdentifier(), String.valueOf(cuboid.getId())),
                                ss);
                layoutDataset = layoutDataset.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid)))
                        .sort(NSparkCubingUtil.getColumns(rowKeys));
                System.out.println("Query cuboid ------------ " + cuboid.getId());
                layoutDataset = dsConvertToOriginal(layoutDataset, cuboid);
                layoutDataset.show(10);

                Dataset<Row> ds = initFlatTable(segment);

                if (!cuboid.isTableIndex()) {
                    ds = CuboidAggregator.agg(ss, ds, cuboid.getOrderedDimensions().keySet(), cuboid.getOrderedMeasures(), null, true);
                }

                Dataset<Row> exceptDs = ds.select(NSparkCubingUtil.getColumns(rowKeys, chooseMeas(cuboid)))
                        .sort(NSparkCubingUtil.getColumns(rowKeys));

                System.out.println("Spark sql ------------ ");
                exceptDs.show(10);

                Assert.assertEquals(layoutDataset.count(), exceptDs.count());
                String msg = SparkQueryTest.checkAnswer(layoutDataset, exceptDs, false);
                Assert.assertNull(msg);
            }
        }
    }

    private Set<Integer> chooseMeas(LayoutEntity cuboid) {
        Set<Integer> measures = Sets.newHashSet();
        for (Map.Entry<Integer, FunctionDesc> entry : cuboid.getOrderedMeasures().entrySet()) {
            String type = entry.getValue().returnType().dataType();
            if (type.equals("hllc") || type.equals("topn") || type.equals("percentile")) {
                continue;
            }
            measures.add(entry.getKey());
        }
        return measures;
    }

    private Dataset<Row> dsConvertToOriginal(Dataset<Row> layoutDs, LayoutEntity entity) {
        Map<Integer, FunctionDesc> orderedMeasures = entity.getOrderedMeasures();

        for (final Map.Entry<Integer, FunctionDesc> entry : orderedMeasures.entrySet()) {
            FunctionDesc functionDesc = entry.getValue();
            if (functionDesc != null) {
                final String[] columns = layoutDs.columns();
                String functionName = functionDesc.returnType().dataType();

                if ("bitmap".equals(functionName)) {
                    final int finalIndex = convertOutSchema(layoutDs, entry.getKey().toString(), DataTypes.LongType);
                    PreciseCountDistinct preciseCountDistinct = new PreciseCountDistinct(null);
                    layoutDs = layoutDs.map((MapFunction<Row, Row>) value -> {
                        Object[] ret = new Object[value.size()];
                        for (int i = 0; i < columns.length; i++) {
                            if (i == finalIndex) {
                                byte[] bytes = (byte[]) value.get(i);
                                Roaring64NavigableMap bitmapCounter = preciseCountDistinct.deserialize(bytes);
                                ret[i] = bitmapCounter.getLongCardinality();
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
}