package com.kylinolap.metadata.project;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kylinolap.metadata.model.realization.DataModelRealizationType;

/**
 * Created by qianzhou on 12/5/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProjectDataModel {

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private DataModelRealizationType type;

    @JsonProperty("realization")
    private String realization;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataModelRealizationType getType() {
        return type;
    }

    public void setType(DataModelRealizationType type) {
        this.type = type;
    }

    public String getRealization() {
        return realization;
    }

    public void setRealization(String realization) {
        this.realization = realization;
    }
}
