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

package org.apache.kylin.cube.kv;

import com.google.common.collect.Maps;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CubeDimEncMap implements IDimensionEncodingMap, java.io.Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CubeDimEncMap.class);

    final private CubeDesc cubeDesc;
    final private CubeSegment seg;
    final private Map<TblColRef, Dictionary<String>> dictionaryMap;
    final private Map<TblColRef, DimensionEncoding> encMap = Maps.newHashMap();

    public CubeDimEncMap(CubeSegment seg) {
        this.cubeDesc = seg.getCubeDesc();
        this.seg = seg;
        this.dictionaryMap = null;
    }

    public CubeDimEncMap(CubeDesc cubeDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        this.cubeDesc = cubeDesc;
        this.seg = null;
        this.dictionaryMap = dictionaryMap;
    }

    @Override
    public DimensionEncoding get(TblColRef col) {
        DimensionEncoding result = encMap.get(col);
        if (result == null) {
            RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(col);
            if (colDesc.isUsingDictionary()) {
                // special dictionary encoding
                Dictionary<String> dict = getDictionary(col);
                if (dict == null) {
                    logger.warn("No dictionary found for dict-encoding column " + col + ", segment " + seg);
                    result = new FixedLenDimEnc(0);
                } else {
                    result = new DictionaryDimEnc(dict);
                }
            } else {
                // normal case
                result = DimensionEncodingFactory.create(colDesc.getEncodingName(), colDesc.getEncodingArgs(), colDesc.getEncodingVersion());
            }
            encMap.put(col, result);
        }
        return result;
    }

    @Override
    public Dictionary<String> getDictionary(TblColRef col) {
        if (seg == null)
            return dictionaryMap.get(col);
        else
            return seg.getDictionary(col);
    }

}
