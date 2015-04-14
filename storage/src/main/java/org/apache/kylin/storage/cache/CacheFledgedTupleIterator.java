package org.apache.kylin.storage.cache;

import com.google.common.base.Preconditions;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.hbase.CubeSegmentTupleIterator;

/**
 * Created by Hongbin Ma(Binmahone) on 4/13/15.
 */
public class CacheFledgedTupleIterator implements IStorageEngine {

    public static final String SUCCESS_QUERY_CACHE = "SuccessQueryCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";

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

    public CacheFledgedTupleIterator(IRealization realization) {
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
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {
        StreamSQLDigest streamSQLDigest = new StreamSQLDigest(sqlDigest, partitionColRef);
        cacheManager.getCache(SUCCESS_QUERY_CACHE).get(streamSQLDigest).getObjectValue();
        return null;
    }
}
