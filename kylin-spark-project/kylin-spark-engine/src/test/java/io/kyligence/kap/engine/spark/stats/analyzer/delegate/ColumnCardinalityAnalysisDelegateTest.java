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


package io.kyligence.kap.engine.spark.stats.analyzer.delegate;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class ColumnCardinalityAnalysisDelegateTest extends NLocalWithSparkSessionTest {

    private static final int COL_IDX = 3;

    private TableDesc tableDesc;

    @Before
    public void setup() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testAnalyze() {
        final ColumnCardinalityAnalysisDelegate delegate = new ColumnCardinalityAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);

        Dataset<Row> sourceData = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, Maps.newHashMap());
        for (final Row row : sourceData.collectAsList()) {
            delegate.analyze(row, row.get(COL_IDX));
        }

        Assert.assertEquals(5, delegate.getCardinality());

    }

    @Test
    public void testReduce() {
        final ColumnCardinalityAnalysisDelegate delegate1 = new ColumnCardinalityAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);
        final HLLCounter mockHLLC1 = mockHLLCounter(3, 7);
        ReflectionTestUtils.setField(delegate1, "hasNullOrBlank", true);
        ReflectionTestUtils.setField(delegate1, "hllCounter", mockHLLC1);

        final ColumnCardinalityAnalysisDelegate delegate2 = new ColumnCardinalityAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);
        final HLLCounter mockHLLC2 = mockHLLCounter(10, 15);
        ReflectionTestUtils.setField(delegate2, "hasNullOrBlank", false);
        ReflectionTestUtils.setField(delegate2, "hllCounter", mockHLLC2);

        final ColumnCardinalityAnalysisDelegate delegate = delegate1.reduce(delegate2);

        Assert.assertEquals(12, delegate.getCardinality());
    }

    private HLLCounter mockHLLCounter(int min, int max) {
        final HLLCounter hllCounter = new HLLCounter(14);
        for (int i = min; i <= max; i++) {
            hllCounter.add(RandomStringUtils.randomAlphanumeric(i));
        }

        return hllCounter;
    }
}
