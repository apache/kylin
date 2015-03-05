package org.apache.kylin.storage.hybrid;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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

        long boundary = hybridInstance.getHistoryRealizationInstance().getDateRangeEnd();
        FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd");
        String boundaryDate = format.format(boundary);

        Collection<TblColRef> filterCols = sqlDigest.filterColumns;

        String modelName = hybridInstance.getModelName();

        MetadataManager metaMgr = getMetadataManager();

        DataModelDesc modelDesc = metaMgr.getDataModelDesc(modelName);

        if (modelDesc.getPartitionDesc() == null || modelDesc.getPartitionDesc().getPartitionDateColumnRef() == null)
            return iterator1;

        String partitionColFull = modelDesc.getPartitionDesc().getPartitionDateColumn();

        String partitionTable = partitionColFull.substring(0, partitionColFull.lastIndexOf("."));
        String partitionCol = partitionColFull.substring(partitionColFull.lastIndexOf(".") + 1);


        TableDesc factTbl = metaMgr.getTableDesc(partitionTable);
        ColumnDesc columnDesc = factTbl.findColumnByName(partitionCol);
        TblColRef partitionColRef = new TblColRef(columnDesc);


        // now search the realtime realization, need add the boundary condition


        CompareTupleFilter compareTupleFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        ColumnTupleFilter columnTupleFilter = new ColumnTupleFilter(partitionColRef);
        ConstantTupleFilter constantTupleFilter = new ConstantTupleFilter(boundaryDate);
        compareTupleFilter.addChild(columnTupleFilter);
        compareTupleFilter.addChild(constantTupleFilter);

        if (sqlDigest.filter == null) {
            sqlDigest.filter = compareTupleFilter;
        } else {
            LogicalTupleFilter logicalTupleFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);

            logicalTupleFilter.addChild(sqlDigest.filter);
            logicalTupleFilter.addChild(compareTupleFilter);

            sqlDigest.filter = logicalTupleFilter;
        }

        boolean addFilterColumn = false, addAllColumn = false;

        if (!sqlDigest.filterColumns.contains(partitionColRef)) {
            sqlDigest.filterColumns.add(partitionColRef);
            addFilterColumn = true;
        }

        if (!sqlDigest.allColumns.contains(partitionColRef)) {
            sqlDigest.allColumns.add(partitionColRef);
            addAllColumn = true;
        }

        ITupleIterator iterator2 = searchRealization(hybridInstance.getRealTimeRealizationInstance(), context, sqlDigest);

        // restore the sqlDigest
        sqlDigest.filter = sqlDigest.filter.getChildren().get(0);

        if(addFilterColumn)
            sqlDigest.filterColumns.remove(partitionColRef);

        if(addAllColumn)
            sqlDigest.allColumns.remove(partitionColRef);

        return new HybridTupleIterator(new ITupleIterator[]{iterator1, iterator2});
//        return new HybridTupleIterator(new ITupleIterator[]{iterator1, ITupleIterator.EMPTY_TUPLE_ITERATOR});
    }

    private ITupleIterator searchRealization(IRealization realization, StorageContext context, SQLDigest sqlDigest) {

        IStorageEngine storageEngine = StorageEngineFactory.getStorageEngine(realization);
        return storageEngine.search(context, sqlDigest);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
