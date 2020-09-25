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

package org.apache.kylin.engine.spark.source;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.metadata.ColumnDesc;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.TableDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@SuppressWarnings("serial")
public class CsvSourceTest extends LocalWithSparkSessionTest {

    private static final String CUBE_NAME = "ci_left_join_cube";
    protected KylinConfig config;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        config = KylinConfig.getInstanceFromEnv();
        KylinBuildEnv.getOrCreate(config);
    }

    @Override
    public void after() {
        KylinBuildEnv.clean();
        super.after();
    }

    @Test
    public void testGetSourceDataFromFactTable() {
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME);
        TableDesc fact = MetadataConverter.extractFactTable(cube);
        List<ColumnDesc> colDescs = Lists.newArrayList();
        Iterator<ColumnDesc> iterator = fact.columns().iterator();
        while (iterator.hasNext()) {
            colDescs.add(iterator.next());
        }

        NSparkCubingEngine.NSparkCubingSource cubingSource = new CsvSource().adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
        Dataset<Row> cubeDS = cubingSource.getSourceData(fact, ss, Maps.newHashMap());
        cubeDS.take(10);
        StructType schema = cubeDS.schema();
        for (int i = 0; i < colDescs.size(); i++) {
            StructField field = schema.fields()[i];
            Assert.assertEquals(field.name(), colDescs.get(i).columnName());
            Assert.assertEquals(field.dataType(), colDescs.get(i).dataType());
        }
    }

    @Test
    public void testGetSourceDataFromLookupTable() {
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME);
        Iterator<TableDesc> iterator = MetadataConverter.extractLookupTable(cube).iterator();
        while (iterator.hasNext()) {
            TableDesc lookup = iterator.next();
            NSparkCubingEngine.NSparkCubingSource cubingSource = new CsvSource().adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
            Dataset<Row> sourceData = cubingSource.getSourceData(lookup, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && !rows.isEmpty());
        }
    }

    @Test
    public void testGetFlatTable() throws IOException {
        System.out.println(getTestConfig().getMetadataUrl());
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME);
        cleanupSegments(CUBE_NAME);
        DataModelDesc model = cube.getModel();
        CubeSegment segment = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(dateToLong("2010-01-01"), dateToLong("2013-01-01")));
        Dataset<Row> ds = initFlatTable(segment);
        ds.show(10);
        StructType schema = ds.schema();

        SegmentInfo segmentInfo = MetadataConverter.getSegmentInfo(segment.getCubeInstance(), segment.getUuid(),
                segment.getName(), segment.getStorageLocationIdentifier());
        scala.collection.immutable.Map<String, String> map = BuildUtils.getColumnIndexMap(segmentInfo);
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(map.apply(field.name())));
        }

        for (LayoutEntity layoutEntity : MetadataConverter.extractEntityList2JavaList(cube)) {
            Set<Integer> dims = layoutEntity.getOrderedDimensions().keySet();
            Column[] modelCols = new Column[dims.size()];
            int index = 0;
            for (int id : dims) {
                modelCols[index] = new Column(String.valueOf(id));
                index++;
            }
            ds.select(modelCols).show(10);
        }
    }
}
