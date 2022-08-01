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
package org.apache.kylin.rest.response;

import org.apache.kylin.metadata.model.ComputedColumnDesc;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ComputedColumnDescResponse {

    @JsonProperty("table_identity")
    private String tableIdentity;

    @JsonProperty("table_alias")
    private String tableAlias;
    @JsonProperty("column_name")
    private String columnName; // the new col name
    @JsonProperty
    private String expression;
    @JsonProperty("inner_expression")
    private String innerExpression; // QueryUtil massaged expression
    @JsonProperty("data_type")
    private String dataType;
    @JsonProperty
    private String comment;

    public static ComputedColumnDescResponse convert(ComputedColumnDesc computedColumnDesc) {
        ComputedColumnDescResponse response = new ComputedColumnDescResponse();
        response.setTableIdentity(computedColumnDesc.getTableIdentity());
        response.setTableAlias(computedColumnDesc.getTableAlias());
        response.setColumnName(computedColumnDesc.getColumnName());
        response.setExpression(computedColumnDesc.getExpression());
        response.setInnerExpression(computedColumnDesc.getInnerExpression());
        response.setDataType(computedColumnDesc.getDatatype());
        response.setComment(computedColumnDesc.getComment());

        return response;
    }

}
