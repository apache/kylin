package com.kylinolap.metadata.project;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.metadata.realization.RealizationType;

/**
 * Created by qianzhou on 12/5/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RealizationEntry {

    @JsonProperty("type")
    private RealizationType type;

    @JsonProperty("realization")
    private String realization;

    public RealizationType getType() {
        return type;
    }

    public void setType(RealizationType type) {
        this.type = type;
    }

    public String getRealization() {
        return realization;
    }

    public void setRealization(String realization) {
        this.realization = realization;
    }
    
    @Override
    public String toString() {
        return "" + type.name() + "." + realization;
    }
}
