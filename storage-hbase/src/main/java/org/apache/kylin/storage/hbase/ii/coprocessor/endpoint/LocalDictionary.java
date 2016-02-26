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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import java.util.Map;

import org.apache.kylin.dimension.Dictionary;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;

import com.google.common.collect.Maps;

/**
 */
public class LocalDictionary implements IDimensionEncodingMap {

    private CoprocessorRowType type;
    private Dictionary<?>[] colDictMap;
    private TableRecordInfoDigest recordInfo;
    private Map<TblColRef, DimensionEncoding> encMap;

    public LocalDictionary(Dictionary<?>[] colDictMap, CoprocessorRowType type, TableRecordInfoDigest recordInfo) {
        this.colDictMap = colDictMap;
        this.type = type;
        this.recordInfo = recordInfo;
        this.encMap = Maps.newHashMap();
    }

    @Override
    public DimensionEncoding get(TblColRef col) {
        DimensionEncoding result = encMap.get(col);
        if (result == null) {
            int len = recordInfo.length(type.getColIndexByTblColRef(col));
            encMap.put(col, result = new FixedLenDimEnc(len));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Dictionary<String> getDictionary(TblColRef col) {
        return (Dictionary<String>) this.colDictMap[type.getColIndexByTblColRef(col)];
    }

}
