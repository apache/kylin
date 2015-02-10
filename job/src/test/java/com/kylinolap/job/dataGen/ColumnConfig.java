package com.kylinolap.job.dataGen;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by honma on 5/29/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnConfig {
    @JsonProperty("columnName")
    private String columnName;
    @JsonProperty("valueSet")
    private ArrayList<String> valueSet;
    @JsonProperty("exclusive")
    private boolean exclusive;
    @JsonProperty("asRange")
    private boolean asRange;

    public boolean isAsRange() {
        return asRange;
    }

    public void setAsRange(boolean asRange) {
        this.asRange = asRange;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ArrayList<String> getValueSet() {
        return valueSet;
    }

    public void setValueSet(ArrayList<String> valueSet) {
        this.valueSet = valueSet;
    }

}
