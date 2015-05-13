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

package org.apache.kylin.storage.test;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.hbase.ScanOutOfLimitException;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class StorageTest extends HBaseMetadataTestCase {

    private IStorageEngine storageEngine;
    private CubeInstance cube;
    private StorageContext context;

    @BeforeClass
    public static void setupResource() throws Exception {
    }

    @AfterClass
    public static void tearDownResource() {
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        cube = cubeMgr.getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_EMPTY");
        Assert.assertNotNull(cube);
        storageEngine = StorageEngineFactory.getStorageEngine(cube);
        String url = KylinConfig.getInstanceFromEnv().getStorageUrl();
        context = new StorageContext();
        context.setConnUrl(url);
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test(expected = ScanOutOfLimitException.class)
    @Ignore
    public void testScanOutOfLimit() {
        context.setThreshold(1);
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();

        search(groups, aggregations, null, context);
    }

    @Test
    public void test01() {
        List<TblColRef> groups = buildGroups();
        List<FunctionDesc> aggregations = buildAggregations();
        TupleFilter filter = buildFilter1(groups.get(0));

        int count = search(groups, aggregations, filter, context);
        assertTrue(count > 0);
    }

    /*
        @Test
        public void test02() {
            List<TblColRef> groups = buildGroups();
            List<FunctionDesc> aggregations = buildAggregations();
            TupleFilter filter = buildFilter2(groups.get(1));

            int count = search(groups, aggregations, filter, context);
            assertTrue(count > 0);
        }

        @Test
        public void test03() {
            List<TblColRef> groups = buildGroups();
            List<FunctionDesc> aggregations = buildAggregations();
            TupleFilter filter = buildAndFilter(groups);

            int count = search(groups, aggregations, filter, context);
            assertTrue(count > 0);
        }

        @Test
        public void test04() {
            List<TblColRef> groups = buildGroups();
            List<FunctionDesc> aggregations = buildAggregations();
            TupleFilter filter = buildOrFilter(groups);

            int count = search(groups, aggregations, filter, context);
            assertTrue(count > 0);
        }

        @Test
        public void test05() {
            List<TblColRef> groups = buildGroups();
            List<FunctionDesc> aggregations = buildAggregations();

            int count = search(groups, aggregations, null, context);
            assertTrue(count > 0);
        }
    */
    private int search(List<TblColRef> groups, List<FunctionDesc> aggregations, TupleFilter filter, StorageContext context) {
        int count = 0;
        ITupleIterator iterator = null;
        try {
            SQLDigest sqlDigest = new SQLDigest("default.test_kylin_fact", filter, null, Collections.<TblColRef> emptySet(), groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations);
            iterator = storageEngine.search(context, sqlDigest, newTupleInfo(groups, aggregations));
            while (iterator.hasNext()) {
                ITuple tuple = iterator.next();
                System.out.println("Tuple = " + tuple);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null)
                iterator.close();
        }
        return count;
    }

    private TupleInfo newTupleInfo(List<TblColRef> groups, List<FunctionDesc> aggregations) {
        TupleInfo info = new TupleInfo();
        int idx = 0;
        
        for (TblColRef col : groups) {
            info.setField(col.getName(), col, idx++);
        }
        
        TableDesc sourceTable = groups.get(0).getColumnDesc().getTable();
        for (FunctionDesc func : aggregations) {
            TblColRef col = new TblColRef(func.newFakeRewriteColumn(sourceTable));
            info.setField(col.getName(), col, idx++);
        }
        
        return info;
    }

    private List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();
        
        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc c1 = ColumnDesc.mockup(t1, 2, "CAL_DT", "string");
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = TableDesc.mockup("DEFAULT.TEST_CATEGORY_GROUPINGS");
        ColumnDesc c2 = ColumnDesc.mockup(t2, 14, "META_CATEG_NAME", "string");
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        return groups;
    }

    private List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef priceCol = new TblColRef(ColumnDesc.mockup(t1, 7, "PRICE", "decimal(19,4)"));
        TblColRef sellerCol = new TblColRef(ColumnDesc.mockup(t1, 9, "SELLER_ID", "bigint"));
        
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(priceCol));
        f1.setParameter(p1);
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("COUNT_DISTINCT");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("SELLER_ID");
        p2.setColRefs(ImmutableList.of(sellerCol));
        f2.setParameter(p2);
        functions.add(f2);

        return functions;
    }

    private CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    private CompareTupleFilter buildFilter2(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("ClothinShoes & Accessories");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    @SuppressWarnings("unused")
    private TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    @SuppressWarnings("unused")
    private TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }
}
