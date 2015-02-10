package com.kylinolap.invertedindex.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.common.util.StringUtil;

/**
 * Created by Hongbin Ma(Binmahone) on 12/26/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class IIDimension {
    @JsonProperty("table")
    private String table;
    @JsonProperty("columns")
    private String[] columns;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }


    public static void capicalizeStrings(List<IIDimension> dimensions) {
        for (IIDimension iiDimension : dimensions) {
            iiDimension.setTable(iiDimension.getTable().toUpperCase());
            StringUtil.toUpperCaseArray(iiDimension.getColumns(), iiDimension.getColumns());
        }
    }

    public static int getColumnCount(List<IIDimension> iiDimensions) {
        int count = 0;
        for (IIDimension iiDimension : iiDimensions) {
            count += iiDimension.getColumns().length;
        }
        return count;
    }

}
