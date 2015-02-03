package org.apache.kylin.job.hadoop.hive;

import java.util.List;

import org.apache.kylin.metadata.model.DataModelDesc;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public interface IJoinedFlatTableDesc {

    public static final String FACT_TABLE_ALIAS = "FACT_TABLE";

    public static final String LOOKUP_TABLE_ALAIS_PREFIX = "LOOKUP_";

    public String getTableName(String jobUUID);

    public List<IntermediateColumnDesc> getColumnList();

    public DataModelDesc getDataModel();

    public DataModelDesc.RealizationCapacity getCapacity();

    public String getTableAlias(String tableName);
}
