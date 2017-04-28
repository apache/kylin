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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigest.SQLCall;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageFactory;
import org.apache.kylin.storage.StorageMockUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class ITStorageTest extends HBaseMetadataTestCase {

    private IStorageQuery storageEngine;
    private CubeInstance cube;
    private StorageContext context;
    private StorageMockUtils mockup;

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
        cube = cubeMgr.getCube("test_kylin_cube_without_slr_left_join_empty");
        Assert.assertNotNull(cube);
        storageEngine = StorageFactory.createQuery(cube);
        String url = KylinConfig.getInstanceFromEnv().getStorageUrl();
        context = new StorageContext();
        context.setConnUrl(url);
        mockup = new StorageMockUtils(cube.getModel());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test01() {
        List<TblColRef> groups = mockup.buildGroups();
        List<FunctionDesc> aggregations = mockup.buildAggregations();
        TupleFilter filter = mockup.buildFilter1(groups.get(0));

        int count = search(groups, aggregations, filter, context);
        assertTrue(count >= 0);
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
            SQLDigest sqlDigest = new SQLDigest("default.test_kylin_fact", filter, null, Collections.<TblColRef> emptySet(), groups, Sets.<TblColRef> newHashSet(), Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations, Collections.<SQLCall> emptyList(), new ArrayList<TblColRef>(), new ArrayList<SQLDigest.OrderEnum>(), false);
            iterator = storageEngine.search(context, sqlDigest, mockup.newTupleInfo(groups, aggregations));
            while (iterator.hasNext()) {
                ITuple tuple = iterator.next();
                System.out.println("Tuple = " + tuple);
                count++;
            }
        } finally {
            if (iterator != null)
                iterator.close();
        }
        return count;
    }

}
