package org.apache.kylin.storage.hybrid;

import com.google.common.collect.Lists;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.tuple.TupleInfo;

import java.util.List;

/**
 */
public class HybridStorageEngine implements IStorageQuery {

    private IRealization[] realizations;
    private IStorageQuery[] storageEngines;

    public HybridStorageEngine(HybridInstance hybridInstance) {
        this.realizations = hybridInstance.getRealizations();
        storageEngines = new IStorageQuery[realizations.length];
        for (int i = 0; i < realizations.length; i++) {
            storageEngines[i] = StorageEngineFactory.getStorageEngine(realizations[i]);
        }
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {
        List<ITupleIterator> tupleIterators = Lists.newArrayList();
        for (int i = 0; i < realizations.length; i++) {
            if (realizations[i].isReady() && realizations[i].isCapable(sqlDigest)) {
                ITupleIterator dataIterator = storageEngines[i].search(context, sqlDigest, returnTupleInfo);
                tupleIterators.add(dataIterator);
            }
        }
        // combine tuple iterator
        return new CompoundTupleIterator(tupleIterators);
    }


}
