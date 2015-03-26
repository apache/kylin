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

package org.apache.kylin.invertedindex.model;

import java.util.List;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IIJoinedFlatTableDesc implements IJoinedFlatTableDesc {

    private IIDesc iiDesc;
    private String tableName;
    private List<IntermediateColumnDesc> columnList = Lists.newArrayList();

    public IIJoinedFlatTableDesc(IIDesc iiDesc) {
        this.iiDesc = iiDesc;
        parseIIDesc();
    }

    private void parseIIDesc() {
        this.tableName = "kylin_intermediate_ii_" + iiDesc.getName();

        int columnIndex = 0;
        for (TblColRef col : iiDesc.listAllColumns()) {
            columnList.add(new IntermediateColumnDesc(String.valueOf(columnIndex), col));
            columnIndex++;
        }
    }

    @Override
    public String getTableName(String jobUUID) {
        return tableName + "_" + jobUUID.replace("-", "_");
    }

    @Override
    public List<IntermediateColumnDesc> getColumnList() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return iiDesc.getModel();
    }

    @Override
    public DataModelDesc.RealizationCapacity getCapacity() {
        return DataModelDesc.RealizationCapacity.SMALL;
    }

}
