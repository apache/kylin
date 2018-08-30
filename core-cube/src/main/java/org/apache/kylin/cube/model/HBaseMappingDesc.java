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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Locale;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class HBaseMappingDesc implements java.io.Serializable {

    @JsonProperty("column_family")
    private HBaseColumnFamilyDesc[] columnFamily;

    // point to the cube instance which contain this HBaseMappingDesc instance.
    private CubeDesc cubeRef;

    public Collection<HBaseColumnDesc> findHBaseColumnByFunction(FunctionDesc function) {
        Collection<HBaseColumnDesc> result = new LinkedList<HBaseColumnDesc>();
        HBaseMappingDesc hbaseMapping = cubeRef.getHbaseMapping();
        if (hbaseMapping == null || hbaseMapping.getColumnFamily() == null) {
            return result;
        }
        for (HBaseColumnFamilyDesc cf : hbaseMapping.getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                for (MeasureDesc m : c.getMeasures()) {
                    if (m.getFunction().equals(function)) {
                        result.add(c);
                    }
                }
            }
        }
        return result;
    }

    public CubeDesc getCubeRef() {
        return cubeRef;
    }

    public void setCubeRef(CubeDesc cubeRef) {
        this.cubeRef = cubeRef;
    }

    public HBaseColumnFamilyDesc[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(HBaseColumnFamilyDesc[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public void init(CubeDesc cubeDesc) {
        cubeRef = cubeDesc;

        for (HBaseColumnFamilyDesc cf : columnFamily) {
            cf.setName(cf.getName().toUpperCase(Locale.ROOT));

            for (HBaseColumnDesc c : cf.getColumns()) {
                c.setQualifier(c.getQualifier().toUpperCase(Locale.ROOT));
                StringUtil.toUpperCaseArray(c.getMeasureRefs(), c.getMeasureRefs());
            }
        }
    }

    public void initAsSeparatedColumns(CubeDesc cubeDesc) {
        cubeRef = cubeDesc;

        int cfNum = cubeDesc.getMeasures().size();
        columnFamily = new HBaseColumnFamilyDesc[cfNum];

        for (int i = 0; i < cfNum; i++) {
            HBaseColumnFamilyDesc cf = new HBaseColumnFamilyDesc();
            HBaseColumnDesc col = new HBaseColumnDesc();
            String measureRef = cubeDesc.getMeasures().get(i).getName();
            col.setMeasureRefs(new String[] { measureRef });
            col.setQualifier("M");
            cf.setColumns(new HBaseColumnDesc[] { col });
            cf.setName("F" + (i + 1));
            columnFamily[i] = cf;
        }
    }

    @Override
    public String toString() {
        return "HBaseMappingDesc [columnFamily=" + Arrays.toString(columnFamily) + "]";
    }

}
