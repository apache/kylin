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

package io.kyligence.kap.engine.spark.mockup;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine.NSparkCubingSource;
import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

@SuppressWarnings("serial")
public class CsvSourceTest extends NLocalWithSparkSessionTest {

    private static final String DEFAULT_TABLE = "DEFAULT.TEST_KYLIN_FACT";

    @Test
    public void testGetTable() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        TableDesc fact = tableMgr.getTableDesc(DEFAULT_TABLE);
        ColumnDesc[] colDescs = fact.getColumns();
        NSparkCubingSource cubingSource = new CsvSource().adaptToBuildEngine(NSparkCubingSource.class);
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
        CsvSource csvSource = new CsvSource();
        ISourceMetadataExplorer sourceMetadataExplorer = csvSource.getSourceMetadataExplorer();
        List<String> databases = sourceMetadataExplorer.listDatabases();
        String database = getProject().toUpperCase();
        Assert.assertTrue(databases.contains(database));
        List<String> tables = sourceMetadataExplorer.listTables(getProject().toUpperCase());
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
        NDataModel model = (NDataModel) df.getModel();

        NCubeJoinedFlatTableDesc flatTableDesc = new NCubeJoinedFlatTableDesc(df.getIndexPlan(),
                new SegmentRange.TimePartitionedSegmentRange(0L, System.currentTimeMillis()), true);
        CreateFlatTable flatTable = new CreateFlatTable(flatTableDesc, null, null, ss, null);
        Dataset<Row> ds = flatTable.generateDataset(false, true);
        ds.show(10);

        StructType schema = ds.schema();
        for (StructField field : schema.fields()) {
            Assert.assertNotNull(model.findColumn(model.getColumnNameByColumnId(Integer.valueOf(field.name()))));
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
        SegmentRange segmentRange = new CsvSource().getSegmentRange("0", "21423423");
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange.getStart().equals(0L) && segmentRange.getEnd().equals(21423423L));
        SegmentRange segmentRange2 = new CsvSource().getSegmentRange("", "");
        Assert.assertTrue(segmentRange2 instanceof SegmentRange.TimePartitionedSegmentRange
                && segmentRange2.getStart().equals(0L) && segmentRange2.getEnd().equals(Long.MAX_VALUE));
    }
}
