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

package org.apache.kylin.storage.parquet.cube;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.TupleInfo;

import java.util.List;

public class DerivedIndexMapping {
    private final int[] tupleIndex;
    private final int[] lookupIndex;
    private final int[] hostIndex;
    private boolean shouldReturnDerived = true;

    public DerivedIndexMapping(CubeDesc.DeriveInfo info, List<TblColRef> dimensions, TupleInfo tupleInfo, TblColRef[] hostCols) {
        TblColRef[] derivedCols = info.columns;

        this.tupleIndex = new int[derivedCols.length];
        this.lookupIndex = new int[derivedCols.length];
        this.hostIndex = new int[hostCols.length];

        boolean allHostsPresent = true;
        for (int i = 0; i < hostCols.length; i++) {
            hostIndex[i] = dimensions.indexOf(hostCols[i]);
            allHostsPresent = allHostsPresent && hostIndex[i] >= 0;
        }

        boolean needCopyDerived = false;
        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef col = derivedCols[i];
            tupleIndex[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            needCopyDerived = needCopyDerived || tupleIndex[i] >= 0;
            lookupIndex[i] = col.getColumnDesc().getZeroBasedIndex();
        }

        if ((allHostsPresent && needCopyDerived) == false)
            this.shouldReturnDerived = false;
    }

    public boolean shouldReturnDerived() {
        return shouldReturnDerived;
    }

    public int numHostColumns() {
        return hostIndex.length;
    }

    public int numDerivedColumns() {
        return tupleIndex.length;
    }

    /**
     * @param i [0, {@link #numHostColumns()})
     * @return row index of the i-th host column
     */
    public int getHostIndex(int i) {
        return hostIndex[i];
    }

    /**
     * @return lookup table's index containing the i-th derived column
     */
    public int getLookupIndex(int i) {
        return lookupIndex[i];
    }

    /**
     * @return tuple index for the i-th derived column
     */
    public int getTupleIndex(int i) {
        return tupleIndex[i];
    }
}
