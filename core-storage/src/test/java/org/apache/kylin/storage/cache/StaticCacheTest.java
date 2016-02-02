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

package org.apache.kylin.storage.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.IdentityUtils;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 */
public class StaticCacheTest {
    @BeforeClass
    public static void setup() {
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.cache.threshold.duration", "0");
    }

    @Test
    public void basicTest() {

        final StorageContext context = new StorageContext();
        final List<TblColRef> groups = StorageMockUtils.buildGroups();
        final List<FunctionDesc> aggregations = StorageMockUtils.buildAggregations();
        final TupleFilter filter = StorageMockUtils.buildFilter1(groups.get(0));
        final SQLDigest sqlDigest = new SQLDigest("default.test_kylin_fact", filter, null, Collections.<TblColRef> emptySet(), groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations, new ArrayList<MeasureDesc>(), new ArrayList<SQLDigest.OrderEnum>());
        final TupleInfo tupleInfo = StorageMockUtils.newTupleInfo(groups, aggregations);

        final List<ITuple> ret = Lists.newArrayList();
        ret.add(new Tuple(tupleInfo));
        ret.add(new Tuple(tupleInfo));
        ret.add(new Tuple(tupleInfo));

        final AtomicInteger underlyingSEHitCount = new AtomicInteger(0);

        CacheFledgedStaticQuery cacheFledgedStaticStorageEngine = new CacheFledgedStaticQuery(new ICachableStorageQuery() {
            @Override
            public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
                underlyingSEHitCount.incrementAndGet();
                return new SimpleTupleIterator(ret.iterator());
            }

            @Override
            public boolean isDynamic() {
                return false;
            }

            @Override
            public Range<Long> getVolatilePeriod() {
                return null;
            }

            @Override
            public String getStorageUUID() {
                return "111ca32a-a33e-4b69-12aa-0bb8b1f8c092";
            }
        });

        ITupleIterator firstIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple, Void> firstResults = new IdentityHashMap<>();
        while (firstIterator.hasNext()) {
            firstResults.put(firstIterator.next(), null);
        }
        firstIterator.close();

        ITupleIterator secondIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple, Void> secondResults = new IdentityHashMap<>();
        while (secondIterator.hasNext()) {
            secondResults.put(secondIterator.next(), null);
        }
        secondIterator.close();

        ITupleIterator thirdIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple, Void> thirdResults = new IdentityHashMap<>();
        while (thirdIterator.hasNext()) {
            thirdResults.put(thirdIterator.next(), null);
        }
        thirdIterator.close();

        Assert.assertEquals(3, firstResults.size());
        IdentityUtils.collectionReferenceEquals(firstResults.keySet(), secondResults.keySet());
        IdentityUtils.collectionReferenceEquals(thirdResults.keySet(), secondResults.keySet());

        Assert.assertEquals(1, underlyingSEHitCount.get());
    }
}
