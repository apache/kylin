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

package org.apache.kylin.metadata.cube.model;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDictionaryDesc implements java.io.Serializable {

    @Getter
    @JsonProperty("id")
    private int id = -1;
    @JsonProperty("reuse")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private int reuseId = -1;
    @JsonProperty("builder")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String builderClass;

    // computed content
    private TblColRef colRef;
    private TblColRef reuseColRef;

    public void init(NDataModel model) {
        colRef = model.getEffectiveCols().get(id);

        // TODO: what's the meaning of id == -1 ? -- ETHER
        if (id != -1) {
            reuseColRef = model.getEffectiveCols().get(reuseId);
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
}
