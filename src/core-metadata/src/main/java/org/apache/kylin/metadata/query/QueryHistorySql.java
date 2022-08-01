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

package org.apache.kylin.metadata.query;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryHistorySql {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("normalized_sql")
    private String normalizedSql;

    @JsonProperty("params")
    private List<QueryHistorySqlParam> params;

    @JsonIgnore
    public String getSqlWithParameterBindingComment() {
        if (CollectionUtils.isEmpty(params)) {
            return sql;
        }
        StringBuilder sb = new StringBuilder(sql);
        sb.append(LINE_SEPARATOR);
        sb.append(LINE_SEPARATOR);
        sb.append("-- [PARAMETER BINDING]");
        sb.append(LINE_SEPARATOR);
        for (QueryHistorySqlParam p : params) {
            sb.append(String.format("-- Binding parameter [%s] as [%s] - [%s]", p.getPos(), p.getDataType(), p.getValue()));
            sb.append(LINE_SEPARATOR);
        }
        sb.append("-- [PARAMETER BINDING END]");

        return sb.toString();
    }
}
