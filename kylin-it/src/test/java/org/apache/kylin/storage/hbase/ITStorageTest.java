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

package org.apache.kylin.storage.hbase;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageFactory;
import org.apache.kylin.storage.cache.StorageMockUtils;
import org.apache.kylin.storage.exception.ScanOutOfLimitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ITStorageTest extends HBaseMetadataTestCase {

    private IStorageQuery storageEngine;
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
        storageEngine = StorageFactory.createQuery(cube);
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
        List<TblColRef> groups = StorageMockUtils.buildGroups();
        List<FunctionDesc> aggregations = StorageMockUtils.buildAggregations();

        search(groups, aggregations, null, context);
    }

    @Test
    public void test01() {
        List<TblColRef> groups = StorageMockUtils.buildGroups();
        List<FunctionDesc> aggregations = StorageMockUtils.buildAggregations();
        TupleFilter filter = StorageMockUtils.buildFilter1(groups.get(0));

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
            SQLDigest sqlDigest = new SQLDigest("default.test_kylin_fact", filter, null, Collections.<TblColRef> emptySet(), groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations, new ArrayList<MeasureDesc>(), new ArrayList<SQLDigest.OrderEnum>());
            iterator = storageEngine.search(context, sqlDigest, StorageMockUtils.newTupleInfo(groups, aggregations));
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

}
