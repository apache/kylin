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

package org.apache.kylin.query.relnode.visitor;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TupleFilterVisitorTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @Test
    public void testMergeToInClause1() {
        TupleFilter originFilter = getMockFilter1();
        TupleFilter resultFilter = TupleFilterVisitor.mergeToInClause(originFilter);
        Assert.assertNotNull(resultFilter);
        Assert.assertEquals(2, resultFilter.getChildren().size());
    }

    @Test
    public void testMergeToInClause2() {
        TupleFilter originFilter = getMockFilter2();
        TupleFilter resultFilter = TupleFilterVisitor.mergeToInClause(originFilter);
        Assert.assertNotNull(resultFilter);
        Assert.assertEquals(2, resultFilter.getChildren().size());
    }

    private TupleFilter getMockFilter1() {
        LogicalTupleFilter ret = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);

        TblColRef colRef1 = TblColRef.newInnerColumn("DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                TblColRef.InnerDataTypeEnum.LITERAL);
        ret.addChildren(getCompareEQFilter(colRef1, "ABIN"));
        ret.addChildren(getCompareEQFilter(colRef1, "Auction"));

        TblColRef colRef2 = TblColRef.newInnerColumn("DEFAULT.TEST_KYLIN_FACT.DEAL_YEAR",
                TblColRef.InnerDataTypeEnum.LITERAL);
        ret.addChildren(getCompareEQFilter(colRef2, "2012"));
        ret.addChildren(getCompareEQFilter(colRef2, "2013"));

        return ret;
    }

    private TupleFilter getMockFilter2() {
        LogicalTupleFilter ret = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);

        TblColRef colRef = TblColRef.newInnerColumn("DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                TblColRef.InnerDataTypeEnum.LITERAL);
        ret.addChildren(getCompareEQFilter(colRef, "ABIN"));
        ret.addChildren(getCompareEQFilter(colRef, "Auction"));

        CompareTupleFilter notInFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NOTIN);
        notInFilter.addChildren(getCompareEQFilter(colRef, "Auction", "Others"));
        ret.addChildren(notInFilter);

        return ret;
    }

    private CompareTupleFilter getCompareEQFilter(TblColRef colRef, String... values) {
        CompareTupleFilter ret = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ret.addChild(new ColumnTupleFilter(colRef));
        ret.addChild(new ConstantTupleFilter(Lists.newArrayList(values)));
        return ret;
    }
}
