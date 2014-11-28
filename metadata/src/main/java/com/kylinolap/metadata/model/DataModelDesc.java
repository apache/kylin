package com.kylinolap.metadata.model;

import org.apache.commons.lang.ArrayUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.metadata.model.realization.TblColRef;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc {
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("fact_table")
    private String factTable;

    @JsonProperty("lookups")
    private LookupDesc[] lookups;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFactTable() {
        return factTable;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable;
    }

    public LookupDesc[] getLookups() {
        return lookups;
    }

    public void setLookups(LookupDesc[] lookups) {
        this.lookups = lookups;
    }
    
    public boolean isFactTable(String factTable) {
        return this.factTable.equalsIgnoreCase(factTable);
    }
    
    public TblColRef findPKByFK(TblColRef fk) {
        assert isFactTable(fk.getTable());

        TblColRef candidate = null;

        for (LookupDesc dim : lookups) {
            JoinDesc join = dim.getJoin();
            if (join == null)
                continue;

            int find = ArrayUtils.indexOf(join.getForeignKeyColumns(), fk);
            if (find >= 0) {
                candidate = join.getPrimaryKeyColumns()[find];
                if (join.getForeignKeyColumns().length == 1) { // is single
                                                               // column join?
                    break;
                }
            }
        }
        return candidate;
    }
    
}
