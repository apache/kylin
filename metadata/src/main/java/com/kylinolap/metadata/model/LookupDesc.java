package com.kylinolap.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class LookupDesc {

    @JsonProperty("table")
    private String table;

    @JsonProperty("join")
    private JoinDesc join;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public JoinDesc getJoin() {
        return join;
    }

    public void setJoin(JoinDesc join) {
        this.join = join;
    }
    
    
}
