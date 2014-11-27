package com.kylinolap.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc {
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("fact_table")
    private String factTable;

    @JsonProperty("lookups")
    private LookupDesc[] lookups;
    
}
