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
package org.apache.kylin.metadata.model;

import org.apache.kylin.metadata.model.tool.CalciteParser;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ComputedColumnDesc implements Serializable {
    @JsonProperty
    private String tableIdentity;
    @JsonProperty
    private String columnName;
    @JsonProperty
    private String expression;
    @JsonProperty
    private String datatype;
    @JsonProperty
    private String comment;

    public void init() {
        Preconditions.checkNotNull(tableIdentity, "tableIdentity is null");
        Preconditions.checkNotNull(columnName, "columnName is null");
        Preconditions.checkNotNull(expression, "expression is null");
        Preconditions.checkNotNull(datatype, "datatype is null");

        Preconditions.checkState(tableIdentity.equals(tableIdentity.trim()), "tableIdentity of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(columnName.equals(columnName.trim()), "columnName of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(datatype.equals(datatype.trim()), "datatype of ComputedColumnDesc has heading/tailing whitespace");

        tableIdentity = tableIdentity.toUpperCase();
        columnName = columnName.toUpperCase();

        if ("true".equals(System.getProperty("needCheckCC")))
            CalciteParser.ensureNoTableNameExists(expression);
    }

    public String getFullName() {
        return tableIdentity + "." + columnName;
    }

    public String getTableIdentity() {
        return tableIdentity;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getExpression() {
        return expression;
    }

    public String getDatatype() {
        return datatype;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ComputedColumnDesc that = (ComputedColumnDesc) o;

        if (!tableIdentity.equals(that.tableIdentity))
            return false;
        if (!columnName.equals(that.columnName))
            return false;
        if (!expression.equals(that.expression))
            return false;
        return datatype.equals(that.datatype);
    }

    @Override
    public int hashCode() {
        int result = tableIdentity.hashCode();
        result = 31 * result + columnName.hashCode();
        result = 31 * result + expression.hashCode();
        result = 31 * result + datatype.hashCode();
        return result;
    }
}
