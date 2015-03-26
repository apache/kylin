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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.util.StringUtil;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 12/26/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DimensionDesc {
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


    public static void capicalizeStrings(List<DimensionDesc> dimensions) {
        for (DimensionDesc dimensionDesc : dimensions) {
            dimensionDesc.setTable(dimensionDesc.getTable().toUpperCase());
            if (dimensionDesc.getColumns() != null) {
                StringUtil.toUpperCaseArray(dimensionDesc.getColumns(), dimensionDesc.getColumns());
            }
        }
    }

    public static int getColumnCount(List<DimensionDesc> dimensionDescs) {
        int count = 0;
        for (DimensionDesc dimensionDesc : dimensionDescs) {
            if (dimensionDesc.getColumns() != null) {
                count += dimensionDesc.getColumns().length;
            }
        }
        return count;
    }

}
