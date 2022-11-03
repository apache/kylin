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

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class NSparkSourceTest extends NLocalWithSparkSessionTest {
    @Test
    public void testGetTable() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");

        Dataset<Row> df = SourceFactory.createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(fact, ss, Maps.newHashMap());

        df.show(10);

        StructType schema = df.schema();
        ColumnDesc[] colDescs = fact.getColumns();
        for (int i = 0; i < colDescs.length; i++) {
            StructField field = schema.fields()[i];
            Assert.assertEquals(field.name(), colDescs[i].getName());
            Assert.assertEquals(field.dataType(), SparderTypeUtil.toSparkType(colDescs[i].getType(), false));
        }
    }

    /**
     * for the issue: https://olapio.atlassian.net/browse/KE-9497
     */
    @Test
    public void testGetSourceData() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
        ColumnDesc[] columns = fact.getColumns();
        columns[0].setName("1d");
        columns[1].setName("234D");
        columns[2].setName("3f");
        columns[3].setName("4F");
        columns[4].setName("5L");
        columns[5].setName("6l");
        fact.setColumns(columns);

        Dataset<Row> df = SourceFactory.createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(fact, ss, Maps.newHashMap());
        ColumnDesc[] colDescs = fact.getColumns();
        for (int i = 0; i < colDescs.length; i++) {
            StructField field = df.schema().fields()[i];
            Assert.assertEquals(field.name(), colDescs[i].getName());
            Assert.assertEquals(field.dataType(), SparderTypeUtil.toSparkType(colDescs[i].getType(), false));
        }
    }

    @Test
    public void testGetSegmentRange() {
        SegmentRange segmentRange = new NSparkDataSource(getTestConfig()).getSegmentRange("0", "21423423");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange.getStart().equals(0L) && segmentRange.getEnd().equals(21423423L));
        SegmentRange segmentRange2 = new NSparkDataSource(getTestConfig()).getSegmentRange("", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }

    @Test
    public void testSupportBuildSnapShotByPartition() {
        assert new NSparkDataSource(getTestConfig()).supportBuildSnapShotByPartition();
    }
}
