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

package org.apache.kylin.engine.spark.mockup;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import org.apache.kylin.engine.spark.builder.CreateFlatTable;
import org.apache.kylin.metadata.cube.model.NCubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class CsvSourceTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_TABLE = "DEFAULT.TEST_KYLIN_FACT";

    @Test
    public void testGetTable() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc fact = tableMgr.getTableDesc(DEFAULT_TABLE);
        ColumnDesc[] colDescs = fact.getColumns();
        NSparkCubingSource cubingSource = new CsvSource(getTestConfig()).adaptToBuildEngine(NSparkCubingSource.class);
        Dataset<Row> df = cubingSource.getSourceData(fact, ss, Maps.newHashMap());
        df.take(10);
        StructType schema = df.schema();
        for (int i = 0; i < colDescs.length; i++) {
            StructField field = schema.fields()[i];
            Assert.assertEquals(field.name(), colDescs[i].getName());
            Assert.assertEquals(field.dataType(), SparderTypeUtil.toSparkType(colDescs[i].getType(), false));
        }
    }

    @Test
    public void testSourceMetadataExplorer() throws Exception {
        CsvSource csvSource = new CsvSource(getTestConfig());
        ISourceMetadataExplorer sourceMetadataExplorer = csvSource.getSourceMetadataExplorer();
        List<String> databases = sourceMetadataExplorer.listDatabases();
        String database = getProject().toUpperCase(Locale.ROOT);
        Assert.assertTrue(databases.contains(database));
        List<String> tables = sourceMetadataExplorer.listTables(getProject().toUpperCase(Locale.ROOT));
        String table = DEFAULT_TABLE.split("\\.")[1];
        Assert.assertTrue(tables.contains(table));
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = sourceMetadataExplorer.loadTableMetadata(database,
                table, getProject());
        TableDesc tableDesc = tableDescTableExtDescPair.getFirst();

        IReadableTable readableTable = csvSource.createReadableTable(tableDesc);
        Assert.assertTrue(readableTable.exists());
    }

    @Test
    public void testGetFlatTable() {
        System.out.println(getTestConfig().getMetadataUrl());
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataModel model = df.getModel();

        NCubeJoinedFlatTableDesc flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan(),
                new SegmentRange.TimePartitionedSegmentRange(0L, System.currentTimeMillis()), true);
        CreateFlatTable flatTable = new CreateFlatTable(flatTableDesc, null, null, ss, null);
        Dataset<Row> ds = flatTable.generateDataset(false, true);
        ds.show(10);

        StructType schema = ds.schema();
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(model.getColumnNameByColumnId(Integer.parseInt(field.name()))));
        }

        Set<Integer> dims = df.getIndexPlan().getEffectiveDimCols().keySet();
        Column[] modelCols = new Column[dims.size()];
        int index = 0;
        for (int id : dims) {
            modelCols[index] = new Column(String.valueOf(id));
            index++;
        }
        ds.select(modelCols).show(10);
    }

    @Test
    public void testGetSegmentRange() {
        SegmentRange segmentRange = new CsvSource(getTestConfig()).getSegmentRange("0", "21423423");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange.getStart().equals(0L) && segmentRange.getEnd().equals(21423423L));
        SegmentRange segmentRange2 = new CsvSource(getTestConfig()).getSegmentRange("", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }
}
