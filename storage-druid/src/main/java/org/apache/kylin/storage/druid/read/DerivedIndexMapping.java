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

package org.apache.kylin.storage.druid.read;

import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.druid.DruidSchema;

public class DerivedIndexMapping {
    private final int[] tupleIndex;
    private final int[] lookupIndex;
    private final int[] hostIndex;

    public DerivedIndexMapping(DeriveInfo info, DruidSchema schema, TupleInfo tupleInfo) {
        TblColRef[] derivedCols = info.columns;
        TblColRef[] hostCols = info.join.getForeignKeyColumns();

        this.tupleIndex = new int[derivedCols.length];
        this.lookupIndex = new int[derivedCols.length];
        this.hostIndex = new int[hostCols.length];

        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef col = derivedCols[i];
            tupleIndex[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            lookupIndex[i] = col.getColumnDesc().getZeroBasedIndex();
        }

        for (int i = 0; i < hostCols.length; i++) {
            hostIndex[i] = schema.getDimensions().indexOf(hostCols[i]);
        }
    }

    public boolean shouldReturnDerived() {
        for (int i = 0; i < tupleIndex.length; i++) {
            if (tupleIndex[i] == -1) {
                return false;
            }
        }
        return true;
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
