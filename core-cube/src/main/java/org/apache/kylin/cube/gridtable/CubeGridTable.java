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

package org.apache.kylin.cube.gridtable;

import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

@SuppressWarnings("rawtypes")
public class CubeGridTable {

    public static Map<TblColRef, Dictionary<?>> getDimensionToDictionaryMap(CubeSegment cubeSeg, long cuboidId) {
        CubeDesc cubeDesc = cubeSeg.getCubeDesc();
        CubeManager cubeMgr = CubeManager.getInstance(cubeSeg.getCubeInstance().getConfig());

        // build a dictionary map
        Map<TblColRef, Dictionary<?>> dictionaryMap = Maps.newHashMap();
        List<TblColRef> dimCols = Cuboid.findById(cubeDesc, cuboidId).getColumns();
        for (TblColRef col : dimCols) {
            Dictionary<?> dictionary = cubeMgr.getDictionary(cubeSeg, col);
            if (dictionary != null) {
                dictionaryMap.put(col, dictionary);
            }
        }
        return dictionaryMap;
    }

    public static GTInfo newGTInfo(CubeSegment cubeSeg, long cuboidId) throws NotEnoughGTInfoException {
        Map<TblColRef, Dictionary<?>> dictionaryMap = getDimensionToDictionaryMap(cubeSeg, cuboidId);
        Cuboid cuboid = Cuboid.findById(cubeSeg.getCubeDesc(), cuboidId);
        for (TblColRef dim : cuboid.getColumns()) {
            if (cubeSeg.getCubeDesc().getRowkey().isUseDictionary(dim)) {
                Dictionary dict = dictionaryMap.get(dim);
                if (dict == null) {
                    throw new NotEnoughGTInfoException();
                }
            }
        }

        return newGTInfo(cubeSeg.getCubeDesc(), cuboidId, dictionaryMap);
    }

    public static GTInfo newGTInfo(CubeDesc cubeDesc, long cuboidId, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(cuboid);

        Map<Integer, Dictionary> dictionaryByColIdx = Maps.newHashMap();
        Map<Integer, Integer> fixLenByColIdx = Maps.newHashMap();

        for (TblColRef dim : cuboid.getColumns()) {
            int colIndex = mapping.getIndexOf(dim);
            if (cubeDesc.getRowkey().isUseDictionary(dim)) {
                Dictionary dict = dictionaryMap.get(dim);
                dictionaryByColIdx.put(colIndex, dict);
            } else {
                int len = cubeDesc.getRowkey().getColumnLength(dim);
                if (len == 0)
                    throw new IllegalStateException();

                fixLenByColIdx.put(colIndex, len);
            }
        }

        GTInfo.Builder builder = GTInfo.builder();
        builder.setTableName("Cuboid " + cuboidId);
        builder.setCodeSystem(new CubeCodeSystem(dictionaryByColIdx, fixLenByColIdx, mapping.getDependentMetricsMap()));
        builder.setColumns(mapping.getDataTypes());
        builder.setPrimaryKey(mapping.getPrimaryKey());
        builder.enableColumnBlock(mapping.getColumnBlocks());
        return builder.build();
    }
}
