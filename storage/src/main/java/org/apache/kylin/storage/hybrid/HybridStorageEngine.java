package org.apache.kylin.storage.hybrid;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.util.DateFormat;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
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

        // search the historic realization
        ITupleIterator iterator1 = searchRealization(hybridInstance.getHistoryRealizationInstance(), context, sqlDigest);

        String modelName = hybridInstance.getModelName();
        MetadataManager metaMgr = getMetadataManager();
        DataModelDesc modelDesc = metaMgr.getDataModelDesc(modelName);

        // if the model isn't partitioned, only query the history
        if (modelDesc.getPartitionDesc() == null || modelDesc.getPartitionDesc().getPartitionDateColumnRef() == null)
            return iterator1;

        TblColRef partitionColRef = modelDesc.getPartitionDesc().getPartitionDateColumnRef();
        DataType partitionColType = partitionColRef.getColumn().getType();

        // add the boundary condition to query real-time

        TupleFilter originalFilter = sqlDigest.filter;
        sqlDigest.filter = createFilterForRealtime(originalFilter, partitionColRef, partitionColType, hybridInstance.getHistoryRealizationInstance().getDateRangeEnd());

        boolean addFilterColumn = false, addAllColumn = false;

        if (!sqlDigest.filterColumns.contains(partitionColRef)) {
            sqlDigest.filterColumns.add(partitionColRef);
            addFilterColumn = true;
        }

        if (!sqlDigest.allColumns.contains(partitionColRef)) {
            sqlDigest.allColumns.add(partitionColRef);
            addAllColumn = true;
        }

        // query real-time
        ITupleIterator iterator2 = searchRealization(hybridInstance.getRealTimeRealizationInstance(), context, sqlDigest);

        // restore the sqlDigest
        sqlDigest.filter = originalFilter;

        if (addFilterColumn)
            sqlDigest.filterColumns.remove(partitionColRef);

        if (addAllColumn)
            sqlDigest.allColumns.remove(partitionColRef);

        // combine history and real-time tuple iterator
        return new HybridTupleIterator(new ITupleIterator[] { iterator1, iterator2 });
    }

    private TupleFilter createFilterForRealtime(TupleFilter originFilter, TblColRef partitionColRef, DataType type, long startDate) {

        String boundaryDate;
        if (type == DataType.getInstance("date")) {
            boundaryDate = DateFormat.formatToDateStr(startDate);
        } else if (type == DataType.getInstance("long")) {
            boundaryDate = String.valueOf(startDate);
        } else {
            throw new IllegalArgumentException("Illegal type for partition column " + type);
        }

        CompareTupleFilter compareTupleFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        ColumnTupleFilter columnTupleFilter = new ColumnTupleFilter(partitionColRef);
        ConstantTupleFilter constantTupleFilter = new ConstantTupleFilter(boundaryDate);
        compareTupleFilter.addChild(columnTupleFilter);
        compareTupleFilter.addChild(constantTupleFilter);

        if (originFilter == null)
            return compareTupleFilter;

        LogicalTupleFilter logicalTupleFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);

        logicalTupleFilter.addChild(originFilter);
        logicalTupleFilter.addChild(compareTupleFilter);

        return logicalTupleFilter;
    }

    private ITupleIterator searchRealization(IRealization realization, StorageContext context, SQLDigest sqlDigest) {

        IStorageEngine storageEngine = StorageEngineFactory.getStorageEngine(realization);
        return storageEngine.search(context, sqlDigest);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
