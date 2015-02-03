package com.kylinolap.job.hadoop.hive;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IntermediateColumnDesc {
    private String id;
    private TblColRef colRef;

    public IntermediateColumnDesc(String id, TblColRef colRef) {
        this.id = id;
        this.colRef = colRef;
    }

    public String getId() {
        return id;
    }

    public String getColumnName() {
        return colRef.getName();
    }

    public String getDataType() {
        return colRef.getDatatype();
    }

    public String getTableName() {
        return colRef.getTable();
    }

    public boolean isSameAs(String tableName, String columnName) {
        return colRef.isSameAs(tableName, columnName);
    }

    public String getCanonicalName() {
        return colRef.getCanonicalName();
    }

}
