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

import java.util.List;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class NSparkCubingSourceInputBySparkDataSourceTest extends NLocalWithSparkSessionTest {

    private final ColumnDesc[] COLUMN_DESCS = new ColumnDesc[2];

    @Before
    public void setup() {
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("id1");
            columnDesc.setDatatype("integer");
            COLUMN_DESCS[0] = columnDesc;
        }
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("str1");
            columnDesc.setDatatype("varchar");
            COLUMN_DESCS[1] = columnDesc;
        }
    }

    @Test
    public void testGetHiveSourceData() {
        try {
            KylinBuildEnv.clean();
            KylinBuildEnv kylinBuildEnv = KylinBuildEnv.getOrCreate(getTestConfig());
            getTestConfig().setProperty("kylin.source.provider.9",
                    "NSparkDataSource");
            getTestConfig().setProperty("kylin.build.resource.read-transactional-table-enabled", "true");
            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
            TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
            Dataset<Row> sourceData = SourceFactory
                    .createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(fact, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && rows.size() > 0);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof org.apache.spark.sql.AnalysisException);
        }
    }

    @Test
    public void testGetHiveSourceDataByTransaction() {
        try {
            KylinBuildEnv.clean();
            KylinBuildEnv kylinBuildEnv = KylinBuildEnv.getOrCreate(getTestConfig());
            getTestConfig().setProperty("kylin.source.provider.9",
                    "NSparkDataSource");
            getTestConfig().setProperty("kylin.build.resource.read-transactional-table-enabled", "true");
            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
            TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
            fact.setTransactional(true);
            Dataset<Row> sourceData = SourceFactory
                    .createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                    .getSourceData(fact, ss, Maps.newHashMap());
            List<Row> rows = sourceData.collectAsList();
            Assert.assertTrue(rows != null && rows.size() > 0);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof org.apache.spark.sql.AnalysisException);
        }
    }

}
