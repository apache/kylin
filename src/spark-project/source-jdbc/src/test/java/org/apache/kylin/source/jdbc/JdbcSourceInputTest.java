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
package org.apache.kylin.source.jdbc;

import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcSourceInputTest extends JdbcTestBase {
    private static SparkSession ss;

    @BeforeClass
    public static void setUp() throws SQLException {
        JdbcTestBase.setUp();
        ss = SparkSession.builder().master("local").getOrCreate();
    }

    @Test
    public void testGetSourceData() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.P_LINEORDER");
        ISource source = SourceFactory.getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return ISourceAware.ID_JDBC;
            }

            @Override
            public KylinConfig getConfig() {
                return getTestConfig();
            }
        });
        NSparkCubingEngine.NSparkCubingSource cubingSource = source
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
        Dataset<Row> sourceData = cubingSource.getSourceData(tableDesc, ss, Maps.newHashMap());
        System.out.println(sourceData.schema());
    }

    @Test
    public void testGetSourceDataCount() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.CUSTOMER");
        ISource source = SourceFactory.getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return ISourceAware.ID_JDBC;
            }

            @Override
            public KylinConfig getConfig() {
                return getTestConfig();
            }
        });
        NSparkCubingEngine.NSparkCubingSource cubingSource = source
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
        Assert.assertTrue(cubingSource instanceof JdbcSourceInput);
        Long countData = cubingSource.getSourceDataCount(tableDesc, ss, Maps.newHashMap());
        Long expectedCount = cubingSource.getSourceData(tableDesc, ss, Maps.newHashMap()).count();
        Assert.assertEquals(expectedCount, countData);
    }
}
