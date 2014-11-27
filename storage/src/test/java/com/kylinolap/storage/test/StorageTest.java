/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.storage.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.ParameterDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.IStorageEngine;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.StorageEngineFactory;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.LogicalTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.filter.TupleFilter.FilterOperatorEnum;
import com.kylinolap.storage.hbase.ScanOutOfLimitException;
import com.kylinolap.storage.tuple.ITuple;
import com.kylinolap.storage.tuple.ITupleIterator;

public class StorageTest extends HBaseMetadataTestCase {

    private IStorageEngine storageEngine;
    private CubeInstance cube;
    private StorageContext context;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        CubeManager cubeMgr = CubeManager.getInstance(this.getTestConfig());
        cube = cubeMgr.getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_EMPTY");
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

    private int search(List<TblColRef> groups, List<FunctionDesc> aggregations, TupleFilter filter, StorageContext context) {
        int count = 0;
        ITupleIterator iterator = null;
        try {
            iterator = storageEngine.search(groups, filter, groups, aggregations, context);
            while (iterator.hasNext()) {
                ITuple tuple = iterator.next();
                System.out.println("Tuple = " + tuple);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return count;
    }

    private List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = new TableDesc();
        t1.setName("TEST_KYLIN_FACT");
        t1.setDatabase("EDW");
        ColumnDesc c1 = new ColumnDesc();
        c1.setName("CAL_DT");
        c1.setTable(t1);
        c1.setDatatype("string");
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = new TableDesc();
        t2.setName("TEST_CATEGORY_GROUPINGS");
        t2.setDatabase("EDW");
        ColumnDesc c2 = new ColumnDesc();
        c2.setName("META_CATEG_NAME");
        c2.setTable(t2);
        c2.setDatatype("string");
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        return groups;
    }

    private List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        f1.setParameter(p1);
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("COUNT_DISTINCT");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("SELLER_ID");
        f2.setParameter(p2);
        functions.add(f2);

        return functions;
    }

    private CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2013-03-10");
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

    private TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    private TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }
}
