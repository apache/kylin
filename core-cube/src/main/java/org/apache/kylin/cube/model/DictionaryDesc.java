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

package org.apache.kylin.cube.model;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DictionaryDesc {

    @JsonProperty("column")
    private String column;
    @JsonProperty("reuse")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reuseColumn;
    @JsonProperty("builder")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String builderClass;

    // computed content
    private TblColRef colRef;
    private TblColRef reuseColRef;

    void init(CubeDesc cubeDesc) {
        DataModelDesc model = cubeDesc.getModel();

        column = column.toUpperCase();
        colRef = model.findColumn(column).getRef();

        if (reuseColumn != null) {
            reuseColumn = reuseColumn.toUpperCase();
            reuseColRef = model.findColumn(reuseColumn).getRef();
        }
    }

    public TblColRef getColumnRef() {
        return colRef;
    }

    public TblColRef getResuseColumnRef() {
        return reuseColRef;
    }

    public String getBuilderClass() {
        return builderClass;
    }

    // for test
    public static DictionaryDesc create(String column, String reuseColumn, String builderClass) {
        DictionaryDesc desc = new DictionaryDesc();
        desc.column = column;
        desc.reuseColumn = reuseColumn;
        desc.builderClass = builderClass;
        return desc;
    }
}
