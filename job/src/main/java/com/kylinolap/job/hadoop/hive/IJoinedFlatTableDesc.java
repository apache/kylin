package com.kylinolap.job.hadoop.hive;

import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.model.DataModelDesc;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public interface IJoinedFlatTableDesc {

    public static final String FACT_TABLE_ALIAS = "FACT_TABLE";

    public static final String LOOKUP_TABLE_ALAIS_PREFIX = "LOOKUP_";

    public String getTableName(String jobUUID);

    public List<IntermediateColumnDesc> getColumnList();

    public DataModelDesc getDataModel();

    public CubeDesc.RealizationCapacity getCapacity();

    public String getTableAlias(String tableName);
}
