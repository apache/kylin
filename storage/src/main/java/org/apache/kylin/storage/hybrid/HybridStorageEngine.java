package org.apache.kylin.storage.hybrid;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigestUtil;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.tuple.TupleInfo;

import javax.annotation.Nullable;

/**
 */
public class HybridStorageEngine implements IStorageEngine {

    private HybridInstance hybridInstance;
    private IStorageEngine historicalStorageEngine;
    private IStorageEngine realtimeStorageEngine;

    public HybridStorageEngine(HybridInstance hybridInstance) {
        this.hybridInstance = hybridInstance;
        this.historicalStorageEngine = StorageEngineFactory.getStorageEngine(this.hybridInstance.getHistoryRealizationInstance());
        this.realtimeStorageEngine = StorageEngineFactory.getStorageEngine(this.hybridInstance.getRealTimeRealizationInstance());
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {

        // search the historic realization
        ITupleIterator historicalDataIterator = this.historicalStorageEngine.search(context, sqlDigest, returnTupleInfo);

        String modelName = hybridInstance.getModelName();
        MetadataManager metaMgr = getMetadataManager();
        DataModelDesc modelDesc = metaMgr.getDataModelDesc(modelName);

        // if the model isn't partitioned, only query the history
        if (modelDesc.getPartitionDesc() == null || modelDesc.getPartitionDesc().getPartitionDateColumnRef() == null)
            return historicalDataIterator;

        TblColRef partitionColRef = modelDesc.getPartitionDesc().getPartitionDateColumnRef();

        ITupleIterator realtimeDataIterator = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, //
                Ranges.atLeast(hybridInstance.getHistoryRealizationInstance().getDateRangeEnd()),//
                new Function<Void, ITupleIterator>() {
                    @Nullable
                    @Override
                    public ITupleIterator apply(@Nullable Void input) {
                        return realtimeStorageEngine.search(context, sqlDigest, returnTupleInfo);
                    }
                });

        // combine history and real-time tuple iterator
        return new CompoundTupleIterator(Lists.newArrayList(historicalDataIterator, realtimeDataIterator));
    }

    @Override
    public Range<Long> getVolatilePeriod() {
        return RangeUtil.merge(historicalStorageEngine.getVolatilePeriod(), realtimeStorageEngine.getVolatilePeriod());
    }

    @Override
    public boolean isDynamic() {
        return true;
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
