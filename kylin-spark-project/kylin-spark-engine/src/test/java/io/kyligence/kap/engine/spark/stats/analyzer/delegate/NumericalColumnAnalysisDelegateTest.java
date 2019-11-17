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


package io.kyligence.kap.engine.spark.stats.analyzer.delegate;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class NumericalColumnAnalysisDelegateTest extends NLocalWithSparkSessionTest {

    private static final int COL_IDX = 8;

    private TableDesc tableDesc;

    @Before
    public void setup() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        tableDesc = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testAnalyze() {
        final NumericalColumnAnalysisDelegate delegate = new NumericalColumnAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);

        Dataset<Row> sourceData = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, Maps.newHashMap());
        for (final Row row : sourceData.collectAsList()) {
            delegate.analyze(row, row.get(COL_IDX));
        }

        Assert.assertEquals(999.84d, delegate.getMax(), 0.00001);
        Assert.assertEquals(-99.79, delegate.getMin(), 0.00001);

    }

    @Test
    public void testReduce() {
        final NumericalColumnAnalysisDelegate delegate1 = new NumericalColumnAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);
        ReflectionTestUtils.setField(delegate1, "maxValue", 999d);
        ReflectionTestUtils.setField(delegate1, "minValue", 111d);

        final NumericalColumnAnalysisDelegate delegate2 = new NumericalColumnAnalysisDelegate(
                tableDesc.getColumns()[COL_IDX]);
        ReflectionTestUtils.setField(delegate2, "maxValue", 888d);
        ReflectionTestUtils.setField(delegate2, "minValue", 0.00d);

        final NumericalColumnAnalysisDelegate delegate = delegate1.reduce(delegate2);
        Assert.assertEquals(999d, delegate.getMax(), 0.00001);
        Assert.assertEquals(0.00d, delegate.getMin(), 0.00001);
    }
}
