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
package org.apache.kylin.rest.util;

import java.io.Serializable;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ModelTriple extends Triple<NDataflow, NDataModel, Object> {
    public static final int SORT_KEY_DATAFLOW = 1;
    public static final int SORT_KEY_DATA_MODEL = 2;
    public static final int SORT_KEY_CALC_OBJECT = 3;

    private NDataflow dataflow;

    private NDataModel dataModel;

    private Serializable calcObject;

    public ModelTriple(NDataflow dataflow, NDataModel dataModel) {
        this.dataflow = dataflow;
        this.dataModel = dataModel;
    }

    @Override
    public NDataflow getLeft() {
        return dataflow;
    }

    @Override
    public NDataModel getMiddle() {
        return dataModel;
    }

    @Override
    public Object getRight() {
        return calcObject;
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
