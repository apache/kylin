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

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.TblColRef;

public class CubeGridTable {

    public static GTInfo newGTInfo(CubeSegment cubeSeg, long cuboidId) {
        Cuboid cuboid = Cuboid.findById(cubeSeg.getCubeDesc(), cuboidId);
        return newGTInfo(cuboid, new CubeDimEncMap(cubeSeg));
    }

    public static GTInfo newGTInfo(CubeDesc cubeDesc, long cuboidId, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidId);
        return newGTInfo(cuboid, new CubeDimEncMap(cubeDesc, dictionaryMap));
    }

    public static GTInfo newGTInfo(Cuboid cuboid, IDimensionEncodingMap dimEncMap) {
        CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(cuboid);

        GTInfo.Builder builder = GTInfo.builder();
        builder.setTableName("Cuboid " + cuboid.getId());
        builder.setCodeSystem(new CubeCodeSystem(mapping.getDimensionEncodings(dimEncMap), mapping.getDependentMetricsMap()));
        builder.setColumns(mapping.getDataTypes());
        builder.setPrimaryKey(mapping.getPrimaryKey());
        builder.enableColumnBlock(mapping.getColumnBlocks());
        return builder.build();
    }
}
