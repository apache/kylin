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
package io.kyligence.kap.engine.spark.source;

import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkCubingSourceInputTest extends NLocalWithSparkSessionTest {

    @Test
    public void testGetSourceData() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc fact = tableMgr.getTableDesc("SSB.P_LINEORDER");
        Dataset<Row> sourceData = SourceFactory.createEngineAdapter(fact, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(fact, ss, Maps.newHashMap());
        List<Row> rows = sourceData.collectAsList();
        Assert.assertTrue(rows != null && rows.size() > 0);
    }
}
