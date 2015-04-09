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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Created by Hongbin Ma(Binmahone) 
 *
 * ii test
 */
public class TsConditionExtractorTest extends LocalFileMetadataTestCase {
    IIInstance ii;
    TableRecordInfo tableRecordInfo;
    CoprocessorFilter filter;
    TableDesc factTableDesc;

    TblColRef caldt;
    TblColRef siteId;

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");
        this.tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        factTableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        this.caldt = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "CAL_DT");
        this.siteId = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID");
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testSimpleFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(caldt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));
        Pair<Long, Long> ret = TsConditionExtractor.extractTsCondition(tableRecordInfo, ii.getAllColumns(), aFilter);
        Assert.assertEquals(946684800000L, ret.getLeft().longValue());
        Assert.assertEquals(null, ret.getRight());
    }


    @Test
    public void testComplextFilter() {
        CompareTupleFilter aFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        aFilter.addChild(new ColumnTupleFilter(caldt));
        aFilter.addChild(new ConstantTupleFilter("2000-01-01"));

        CompareTupleFilter bFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        bFilter.addChild(new ColumnTupleFilter(caldt));
        bFilter.addChild(new ConstantTupleFilter("2000-01-03"));

        CompareTupleFilter cFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.LTE);
        cFilter.addChild(new ColumnTupleFilter(caldt));
        cFilter.addChild(new ConstantTupleFilter("2000-01-02"));

        CompareTupleFilter dFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        dFilter.addChild(new ColumnTupleFilter(siteId));
        dFilter.addChild(new ConstantTupleFilter("0"));

        LogicalTupleFilter rootFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        rootFilter.addChildren(Lists.newArrayList(aFilter, bFilter,cFilter, dFilter));

        Pair<Long, Long> ret = TsConditionExtractor.extractTsCondition(tableRecordInfo, ii.getAllColumns(), rootFilter);

        Assert.assertEquals(946684800000L, ret.getLeft().longValue());
        Assert.assertEquals(946771200000L, ret.getRight().longValue());
    }
}
