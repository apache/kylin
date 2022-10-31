package org.apache.kylin.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class QueryHistoryProjectInfo {

    public static final String PROJECT_NAME = "project_name";

    @JsonProperty(PROJECT_NAME)
    private String projectName;

    private long count;

}
