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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import java.io.IOException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.cache.TsConditionExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 *
 * ii test
 */
public class TsConditionExtractorTest extends LocalFileMetadataTestCase {
    IIInstance ii;
    TableRecordInfo tableRecordInfo;
    TableDesc factTableDesc;

    TblColRef calDt;
    TblColRef siteId;

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");
        if (ii.getFirstSegment() == null) {
            IISegment segment = IIManager.getInstance(getTestConfig()).buildSegment(ii, 0, System.currentTimeMillis());
            ii.getSegments().add(segment);
        }
        this.tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        this.factTableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        this.calDt = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "CAL_DT");
        this.siteId = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID");
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testSimpleFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(calDt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        Range<Long> range = TsConditionExtractor.extractTsCondition(ii.getAllColumns().get(tableRecordInfo.getTimestampColumn()), aFilter);
        Assert.assertEquals(946684800000L, range.lowerEndpoint().longValue());
        Assert.assertEquals(BoundType.OPEN, range.lowerBoundType());
        Assert.assertTrue(!range.hasUpperBound());
    }

    @Test
    public void testComplexFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(calDt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        CompareTupleFilter bFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        bFilter.addChild(new ColumnTupleFilter(calDt));
        bFilter.addChild(new ConstantTupleFilter("2000-01-03"));

        CompareTupleFilter cFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        cFilter.addChild(new ColumnTupleFilter(calDt));
        cFilter.addChild(new ConstantTupleFilter("2000-01-02"));

        CompareTupleFilter dFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        dFilter.addChild(new ColumnTupleFilter(siteId));
        dFilter.addChild(new ConstantTupleFilter("0"));

        LogicalTupleFilter rootFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        rootFilter.addChildren(Lists.newArrayList(aFilter, bFilter, cFilter, dFilter));

        Range<Long> range = TsConditionExtractor.extractTsCondition(ii.getAllColumns().get(tableRecordInfo.getTimestampColumn()), rootFilter);

        Assert.assertEquals(946684800000L, range.lowerEndpoint().longValue());
        Assert.assertEquals(946771200000L, range.upperEndpoint().longValue());
        Assert.assertEquals(BoundType.OPEN, range.lowerBoundType());
        Assert.assertEquals(BoundType.CLOSED, range.upperBoundType());
    }

    @Test
    public void testMoreComplexFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(calDt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        CompareTupleFilter bFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        bFilter.addChild(new ColumnTupleFilter(calDt));
        bFilter.addChild(new ConstantTupleFilter("2000-01-04"));

        CompareTupleFilter cFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        cFilter.addChild(new ColumnTupleFilter(calDt));
        cFilter.addChild(new ConstantTupleFilter("2000-01-03"));

        CompareTupleFilter dFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        dFilter.addChild(new ColumnTupleFilter(siteId));
        dFilter.addChild(new ConstantTupleFilter("0"));

        LogicalTupleFilter subRoot = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        subRoot.addChildren(Lists.newArrayList(aFilter, bFilter, cFilter, dFilter));

        CompareTupleFilter outFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        outFilter.addChild(new ColumnTupleFilter(calDt));
        outFilter.addChild(new ConstantTupleFilter("2000-01-02"));

        LogicalTupleFilter root = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        root.addChildren(Lists.newArrayList(subRoot, outFilter));

        Range<Long> range = TsConditionExtractor.extractTsCondition(ii.getAllColumns().get(tableRecordInfo.getTimestampColumn()), root);

        Assert.assertEquals(946684800000L, range.lowerEndpoint().longValue());
        Assert.assertEquals(946771200000L, range.upperEndpoint().longValue());
        Assert.assertEquals(BoundType.OPEN, range.lowerBoundType());
        Assert.assertEquals(BoundType.CLOSED, range.upperBoundType());
    }

    @Test
    public void testComplexConflictFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(calDt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        CompareTupleFilter bFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        bFilter.addChild(new ColumnTupleFilter(calDt));
        bFilter.addChild(new ConstantTupleFilter("1999-01-03"));

        CompareTupleFilter cFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        cFilter.addChild(new ColumnTupleFilter(calDt));
        cFilter.addChild(new ConstantTupleFilter("2000-01-02"));

        CompareTupleFilter dFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        dFilter.addChild(new ColumnTupleFilter(siteId));
        dFilter.addChild(new ConstantTupleFilter("0"));

        LogicalTupleFilter rootFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        rootFilter.addChildren(Lists.newArrayList(aFilter, bFilter, cFilter, dFilter));

        Range<Long> range = TsConditionExtractor.extractTsCondition(ii.getAllColumns().get(tableRecordInfo.getTimestampColumn()), rootFilter);

        Assert.assertTrue(range == null);

    }

    @Test
    public void testMoreComplexConflictFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(calDt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        CompareTupleFilter bFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        bFilter.addChild(new ColumnTupleFilter(calDt));
        bFilter.addChild(new ConstantTupleFilter("2000-01-04"));

        CompareTupleFilter cFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        cFilter.addChild(new ColumnTupleFilter(calDt));
        cFilter.addChild(new ConstantTupleFilter("2000-01-03"));

        CompareTupleFilter dFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        dFilter.addChild(new ColumnTupleFilter(siteId));
        dFilter.addChild(new ConstantTupleFilter("0"));

        LogicalTupleFilter subRoot = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        subRoot.addChildren(Lists.newArrayList(aFilter, bFilter, cFilter, dFilter));

        CompareTupleFilter outFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        outFilter.addChild(new ColumnTupleFilter(calDt));
        outFilter.addChild(new ConstantTupleFilter("1999-01-02"));

        LogicalTupleFilter root = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        root.addChildren(Lists.newArrayList(subRoot, outFilter));

        Range<Long> range = TsConditionExtractor.extractTsCondition(ii.getAllColumns().get(tableRecordInfo.getTimestampColumn()), root);

        Assert.assertTrue(range == null);

    }
}
