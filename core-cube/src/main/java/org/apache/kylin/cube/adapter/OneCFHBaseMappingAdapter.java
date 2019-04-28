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

package org.apache.kylin.cube.adapter;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.util.List;

/**
 * All metrics are assigned in one column family. Each metric is assigned into one qualifier
 */
public class OneCFHBaseMappingAdapter extends AbstractHBaseMappingAdapter {

    public OneCFHBaseMappingAdapter(KylinConfig config) {
        super(config);
    }

    @Override
    public HBaseMappingDesc getHBaseMappingOnlyOnce(CubeDesc cubeDesc) {
        // use to mapping no memory hungry measures
        HBaseColumnFamilyDesc finalColumnFamily = new HBaseColumnFamilyDesc();
        finalColumnFamily.setName(hBaseColumnFamilyNamePrefix + 1);

        List<HBaseColumnDesc> columnList = Lists.newArrayListWithExpectedSize(cubeDesc.getMeasures().size());
        List<MeasureDesc> measureDescs = Lists.newArrayList(cubeDesc.getMeasures());
//        measureDescs.sort(Comparator.comparingLong(MeasureDesc::getCreatedTime));
        for (MeasureDesc measure : measureDescs) {
            HBaseColumnDesc column = getHBaseColumnDescWithName(hBaseColumnQualifierPrefix + (columnList.size() + 1));
            column.setMeasureRefs(new String[]{measure.getName()});
            columnList.add(column);
        }

        finalColumnFamily.setColumns(columnList.toArray(new HBaseColumnDesc[columnList.size()]));

        HBaseMappingDesc hbaseMapping = new HBaseMappingDesc();
        hbaseMapping.setColumnFamily(new HBaseColumnFamilyDesc[]{finalColumnFamily});
        return hbaseMapping;
    }

    @Override
    public HBaseMappingDesc addMeasure(CubeDesc newDesc, List<String> needAddMeasure) {
        // just add column on by one
        HBaseMappingDesc oldMappingCopy = copy(getCubeDescManager().getCubeDesc(newDesc.getName()).getHbaseMapping());
        HBaseColumnFamilyDesc columnFamilyDesc = oldMappingCopy.getColumnFamily()[0];
        List<HBaseColumnDesc> hBaseColumnDescs = Lists.newArrayList(columnFamilyDesc.getColumns());
        for (String measureName : needAddMeasure) {
            hBaseColumnDescs.add(getColumnContainSingleMeasure(measureName, hBaseColumnQualifierPrefix + (findMaxSuffixOfColumn(hBaseColumnDescs) + 1)));
        }
        columnFamilyDesc.setColumns(hBaseColumnDescs.toArray(new HBaseColumnDesc[0]));

        return oldMappingCopy;
    }
}
