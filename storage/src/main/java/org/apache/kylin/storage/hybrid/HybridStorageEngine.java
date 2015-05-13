package org.apache.kylin.storage.hybrid;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigestUtil;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.tuple.TupleInfo;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ranges;

/**
 */
public class HybridStorageEngine implements IStorageEngine {

    private HybridInstance hybridInstance;

    public HybridStorageEngine(HybridInstance hybridInstance) {
        this.hybridInstance = hybridInstance;
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {

        // search the historic realization
        ITupleIterator iterator1 = searchRealization(hybridInstance.getHistoryRealizationInstance(), context, sqlDigest, returnTupleInfo);

        String modelName = hybridInstance.getModelName();
        MetadataManager metaMgr = getMetadataManager();
        DataModelDesc modelDesc = metaMgr.getDataModelDesc(modelName);

        // if the model isn't partitioned, only query the history
        if (modelDesc.getPartitionDesc() == null || modelDesc.getPartitionDesc().getPartitionDateColumnRef() == null)
            return iterator1;

        TblColRef partitionColRef = modelDesc.getPartitionDesc().getPartitionDateColumnRef();

        ITupleIterator iterator2 = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, //
                Ranges.atLeast(hybridInstance.getHistoryRealizationInstance().getDateRangeEnd()),//
                new Function<Void, ITupleIterator>() {
                    @Nullable
                    @Override
                    public ITupleIterator apply(@Nullable Void input) {
                        ITupleIterator iterator2 = searchRealization(hybridInstance.getRealTimeRealizationInstance(), context, sqlDigest, returnTupleInfo);
                        return iterator2;
                    }
                });

        // combine history and real-time tuple iterator
        return new CompoundTupleIterator(Lists.newArrayList(iterator1, iterator2));
    }

    private ITupleIterator searchRealization(IRealization realization, StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        IStorageEngine storageEngine = StorageEngineFactory.getStorageEngine(realization, false);
        return storageEngine.search(context, sqlDigest, returnTupleInfo);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
