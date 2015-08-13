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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.filter.BitMapFilterEvaluator;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorRowType;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * <p/>
 * an adapter
 */
public class SliceBitMapProvider implements BitMapFilterEvaluator.BitMapProvider {

    private Slice slice;
    private CoprocessorRowType type;

    public SliceBitMapProvider(Slice slice, CoprocessorRowType type) {
        this.slice = slice;
        this.type = type;
    }

    @Override
    public ConciseSet getBitMap(TblColRef col, Integer startId, Integer endId) {
        return slice.getColumnValueContainer(type.getColIndexByTblColRef(col)).getBitMap(startId, endId);
    }

    @Override
    public int getRecordCount() {
        return this.slice.getRecordCount();
    }

    @Override
    public int getMaxValueId(TblColRef col) {
        return slice.getColumnValueContainer(type.getColIndexByTblColRef(col)).getMaxValueId();
    }
}
