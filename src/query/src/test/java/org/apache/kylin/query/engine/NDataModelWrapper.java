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
package org.apache.kylin.query.engine;

import java.util.List;

import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;

import lombok.EqualsAndHashCode;

/**
 * 29/03/2024 hellozepp(lisheng.zhanglin@163.com)
 */
@EqualsAndHashCode(callSuper = false)
public class NDataModelWrapper extends NDataModel {
    @EqualsAndHashCode.Include
    @JsonGetter("computed_columns")
    @JsonInclude(JsonInclude.Include.NON_NULL) // output to frontend
    public List<ComputedColumnDesc> getComputedColumnDescs() {
        return this.computedColumnDescs;
    }

    @JsonSetter("computed_columns")
    public void setComputedColumnDescs(List<ComputedColumnDesc> computedColumnDescs) {
        this.computedColumnDescs = computedColumnDescs;
    }

}
