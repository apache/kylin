package org.apache.kylin.storage.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.kylin.common.util.IdentityUtils;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.cache.CacheFledgedStaticStorageEngine;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Hongbin Ma(Binmahone) on 5/14/15.
 */
public class StaticCacheTest {

    @Test
    public void basicTest() {

        final StorageContext context = new StorageContext();
        final List<TblColRef> groups = StorageMockUtils.buildGroups();
        final List<FunctionDesc> aggregations = StorageMockUtils.buildAggregations();
        final TupleFilter filter = StorageMockUtils.buildFilter1(groups.get(0));
        final SQLDigest sqlDigest = new SQLDigest("default.test_kylin_fact", filter, null, Collections.<TblColRef> emptySet(), groups, Collections.<TblColRef> emptySet(), Collections.<TblColRef> emptySet(), aggregations);
        final TupleInfo tupleInfo = StorageMockUtils.newTupleInfo(groups, aggregations);

        final List<ITuple> ret = Lists.newArrayList();
        ret.add(new Tuple(tupleInfo));
        ret.add(new Tuple(tupleInfo));
        ret.add(new Tuple(tupleInfo));

        final AtomicInteger underlyingSEHitCount = new AtomicInteger(0);

        CacheFledgedStaticStorageEngine cacheFledgedStaticStorageEngine = new CacheFledgedStaticStorageEngine(new IStorageEngine() {
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
        });

        ITupleIterator firstIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple, Void> firstResults = new IdentityHashMap<>();
        while (firstIterator.hasNext()) {
            firstResults.put(firstIterator.next(), null);
        }
        firstIterator.close();

        ITupleIterator secondIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple,Void> secondResults = new IdentityHashMap<>();
        while(secondIterator.hasNext())
        {
            secondResults.put(secondIterator.next(),null);
        }
        secondIterator.close();

        ITupleIterator thirdIterator = cacheFledgedStaticStorageEngine.search(context, sqlDigest, tupleInfo);
        IdentityHashMap<ITuple,Void> thirdResults = new IdentityHashMap<>();
        while(thirdIterator.hasNext())
        {
            thirdResults.put(thirdIterator.next(),null);
        }
        thirdIterator.close();

        Assert.assertEquals(3, firstResults.size());
        IdentityUtils.collectionReferenceEquals(firstResults.keySet(), secondResults.keySet());
        IdentityUtils.collectionReferenceEquals(thirdResults.keySet(), secondResults.keySet());

        Assert.assertEquals(1, underlyingSEHitCount.get());
    }
}
