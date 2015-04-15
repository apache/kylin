package org.apache.kylin.storage.cache;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.*;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.TsConditionExtractor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 4/13/15.
 */
public class CacheFledgedTupleIterator implements IStorageEngine {

    public static final String SUCCESS_QUERY_CACHE = "SuccessQueryCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";//TODO

    static CacheManager cacheManager;

    static {
        cacheManager = CacheManager.create();

        //Create a Cache specifying its configuration.
        Cache successCache = new Cache(new CacheConfiguration(SUCCESS_QUERY_CACHE, 0).//
                memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU).//
                eternal(false).//
                timeToIdleSeconds(86400).//
                diskExpiryThreadIntervalSeconds(0).//
                maxBytesLocalHeap(500, MemoryUnit.MEGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.LOCALTEMPSWAP)));

        cacheManager.addCache(successCache);
    }

    private TblColRef partitionColRef;
    private IRealization realization;

    public CacheFledgedTupleIterator(IRealization realization) {
        this.realization = realization;

        Preconditions.checkArgument(realization.getType() != RealizationType.CUBE, "Cube realization does not need dynamic cache!");
        String modelName = realization.getModelName();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getDataModelDesc(modelName);
        PartitionDesc partitionDesc = dataModelDesc.getPartitionDesc();
        Preconditions.checkArgument(partitionDesc != null, "PartitionDesc for " + realization + " is null!");
        assert partitionDesc != null;
        partitionColRef = partitionDesc.getPartitionDateColumnRef();
        Preconditions.checkArgument(partitionColRef != null, "getPartitionDateColumnRef for " + realization + " is null");
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest) {
        StreamSQLDigest streamSQLDigest = new StreamSQLDigest(sqlDigest, partitionColRef);
        StreamSQLResult cachedResult = (StreamSQLResult) cacheManager.getCache(SUCCESS_QUERY_CACHE).get(streamSQLDigest).getObjectValue();

        ITupleIterator ret;

        if (cachedResult != null) {
            Range<Long> tsRange = TsConditionExtractor.extractTsCondition(partitionColRef, sqlDigest.filter);
            Range<Long> reusePeriod = cachedResult.getReusableResults(tsRange);
            if (reusePeriod != null) {
                List<Range<Long>> remainings = RangeUtil.remove(tsRange, reusePeriod);
                if (remainings.size() == 1) {
                    SimpleTupleIterator reusedTuples = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                    Range<Long> remaining = remainings.get(0);
                    ITupleIterator freshTuples = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, remaining, new Function<Void, ITupleIterator>() {
                        @Nullable
                        @Override
                        public ITupleIterator apply(Void input) {
                            return StorageEngineFactory.getStorageEngine(realization, false).search(context, sqlDigest);
                        }
                    });
                    ret = new CompoundTupleIterator(Lists.newArrayList(reusedTuples, freshTuples));
                    //TODO:update cache
                    return ret;//cache successfully reused
                }
            }
        }

        //cache cannot reuse case:
        ret = StorageEngineFactory.getStorageEngine(realization, false).search(context, sqlDigest);
        //TODO:update cache
        return ret;

    }
}
