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

import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DictionaryDesc implements java.io.Serializable {

    @JsonProperty("column")
    private String column;
    @JsonProperty("reuse")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reuseColumn;
    @JsonProperty("builder")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String builderClass;

    //for tiretree global domain dic
    @JsonProperty("cube")
    private String cube;

    //for tiretree global domain dic
    @JsonProperty("model")
    private String model;


    // computed content
    private TblColRef colRef;
    private TblColRef reuseColRef;

    void init(CubeDesc cubeDesc) {
        DataModelDesc model = cubeDesc.getModel();

        column = column.toUpperCase(Locale.ROOT);
        colRef = model.findColumn(column);

        if (reuseColumn != null) {
            reuseColumn = reuseColumn.toUpperCase(Locale.ROOT);
            reuseColRef = model.findColumn(reuseColumn);
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

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    public String getReuseColumn() {
        return reuseColumn;
    }

    /**
     * check if the col is tiretree global domain dic
     * @return
     */
    public boolean isDomain() {
        if (Objects.isNull(reuseColRef) && Objects.nonNull(reuseColumn)) {
            return true;
        }
        return false;
    }


    // for test
    public static DictionaryDesc create(String column, String reuseColumn, String builderClass) {
        DictionaryDesc desc = new DictionaryDesc();
        desc.column = column;
        desc.reuseColumn = reuseColumn;
        desc.builderClass = builderClass;
        return desc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DictionaryDesc that = (DictionaryDesc) o;
        return Objects.equals(column, that.column) &&
                Objects.equals(reuseColumn, that.reuseColumn) &&
                Objects.equals(builderClass, that.builderClass) &&
                Objects.equals(cube, that.cube) &&
                Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, reuseColumn, builderClass, cube, model);
    }
}
