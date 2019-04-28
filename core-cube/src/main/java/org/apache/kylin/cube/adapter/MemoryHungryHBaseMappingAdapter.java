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
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;

import java.util.Comparator;
import java.util.List;

public class MemoryHungryHBaseMappingAdapter extends AbstractHBaseMappingAdapter {

    public MemoryHungryHBaseMappingAdapter(KylinConfig config) {
        super(config);
    }

    @Override
    public HBaseMappingDesc getHBaseMappingOnlyOnce(CubeDesc cubeDesc) {
        List<HBaseColumnFamilyDesc> hbaseCFDescs = Lists.newArrayListWithCapacity(4);

        List<String> normMeasures = Lists.newArrayListWithCapacity(cubeDesc.getMeasures().size());
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            MeasureType<?> measureType = getMeasureType(measure);
            if (measureType.isMemoryHungry()) {
                HBaseColumnFamilyDesc memoryHungryCF = getSingleColumnCFContain(measure,
                        hBaseColumnFamilyNamePrefix + (hbaseCFDescs.size() + 1));
                hbaseCFDescs.add(memoryHungryCF);
            } else {
                normMeasures.add(measure.getName());
            }
        }

        if (normMeasures.size() > 0) {
            // use to mapping no memory hungry measures
            HBaseColumnFamilyDesc normalCF = new HBaseColumnFamilyDesc();
            normalCF.setName(hBaseColumnFamilyNamePrefix + (hbaseCFDescs.size() + 1));
            HBaseColumnDesc normColumn = getHBaseColumnDescWithName(hBaseColumnQualifierPrefix);
            normColumn.setMeasureRefs(normMeasures.toArray(new String[normMeasures.size()]));
            normalCF.setColumns(new HBaseColumnDesc[]{normColumn});
            hbaseCFDescs.add(normalCF);
        }

        HBaseMappingDesc hbaseMapping = new HBaseMappingDesc();
        hbaseMapping.setColumnFamily(hbaseCFDescs.toArray(new HBaseColumnFamilyDesc[hbaseCFDescs.size()]));
        return hbaseMapping;

    }

    @Override
    public HBaseMappingDesc addMeasure(CubeDesc newDesc, List<String> needAddMeasure) {
        if (needAddMeasure == null) {
            return getHBaseMappingOnlyOnce(newDesc);
        }
        HBaseMappingDesc oldMappingCopy = copy(getCubeDescManager().getCubeDesc(newDesc.getName()).getHbaseMapping());
        if (needAddMeasure.size() == 0) {
            return oldMappingCopy;
        }
        List<HBaseColumnFamilyDesc> cfs = Lists.newArrayList(oldMappingCopy.getColumnFamily());
        // first element always contain the min number of norm measure
        List<Pair<Integer, Integer>> normCFIndex = getNormCFIndexInOldMapping(newDesc.getName());
        for (String measureName : needAddMeasure) {
            MeasureDesc measureDesc = getMeasureByName(newDesc, measureName);
            if (getMeasureType(measureDesc).isMemoryHungry()) {
                // add new CF
                HBaseColumnFamilyDesc memoryCF = getSingleColumnCFContain(measureDesc, hBaseColumnFamilyNamePrefix + (findMaxSuffixOfCF(cfs) + 1));
                cfs.add(memoryCF);
            } else {
                // find norm CF then add new column to contain this measure,
                // if don't have an norm CF or achieve max measure count, create one
                boolean needAddCF = normCFIndex.size() == 0
                        || (normCFIndex.get(0).getSecond() >= config.getMaxMeasureNumberInOneColumn() && cfs.size() <= config.getMaxColumnFamilyNumber());
                if (needAddCF) {
                    HBaseColumnFamilyDesc normCF = getSingleColumnCFContain(measureDesc, hBaseColumnFamilyNamePrefix + (findMaxSuffixOfCF(cfs) + 1));
                    cfs.add(normCF);
                    normCFIndex.add(new Pair<>(cfs.size() - 1, 1));
                } else {
                    Pair<Integer, Integer> idxCntPair = normCFIndex.get(0);
                    Integer idx = idxCntPair.getFirst();
                    List<HBaseColumnDesc> normColumn = Lists.newArrayList(cfs.get(idx).getColumns());
                    HBaseColumnDesc newColumn = getColumnContainSingleMeasure(measureDesc, hBaseColumnQualifierPrefix + (findMaxSuffixOfColumn(normColumn) + 1));
                    normColumn.add(newColumn);
                    cfs.get(idx).setColumns(normColumn.toArray(new HBaseColumnDesc[0]));
                    idxCntPair.setSecond(idxCntPair.getSecond() + 1);
                }
                normCFIndex.sort(Comparator.comparingInt(Pair::getSecond));
            }
        }
        oldMappingCopy.setColumnFamily(cfs.toArray(new HBaseColumnFamilyDesc[0]));
        return oldMappingCopy;
    }
}
