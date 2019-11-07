/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.stream.core.source;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MessageParserInfo {
    @JsonProperty("ts_col_name")
    private String tsColName;

    @JsonProperty("ts_parser")
    private String tsParser;

    @JsonProperty("ts_pattern")
    private String tsPattern;

    @JsonProperty("format_ts")
    private boolean formatTs;

    @JsonProperty("field_mapping")
    private Map<String, String> columnToSourceFieldMapping;

    public String getTsColName() {
        return tsColName;
    }

    public void setTsColName(String tsColName) {
        this.tsColName = tsColName;
    }

    public String getTsParser() {
        return tsParser;
    }

    public void setTsParser(String tsParser) {
        this.tsParser = tsParser;
    }

    public String getTsPattern() {
        return tsPattern;
    }

    public void setTsPattern(String tsPattern) {
        this.tsPattern = tsPattern;
    }

    public boolean isFormatTs() {
        return formatTs;
    }

    public void setFormatTs(boolean formatTs) {
        this.formatTs = formatTs;
    }

    public Map<String, String> getColumnToSourceFieldMapping() {
        return columnToSourceFieldMapping;
    }

    public void setColumnToSourceFieldMapping(Map<String, String> columnToSourceFieldMapping) {
        this.columnToSourceFieldMapping = columnToSourceFieldMapping;
    }

}
