package org.apache.kylin.storage.hybrid;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shaoshi on 2/13/15.
 */
public class HybridStorageEngine implements IStorageEngine {


    private static final Logger logger = LoggerFactory.getLogger(HybridStorageEngine.class);

    private HybridInstance hybridInstance;


    public HybridStorageEngine(HybridInstance hybridInstance) {
        this.hybridInstance = hybridInstance;

    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {

        long conditionBoundry = hybridInstance.getHistoryRealizationInstance().getDateRangeEnd();

        TupleFilter filter = sqlDigest.filter;

        return null;
    }
}
