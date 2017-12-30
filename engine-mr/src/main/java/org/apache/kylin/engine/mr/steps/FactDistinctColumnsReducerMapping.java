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

package org.apache.kylin.engine.mr.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Reducers play different roles based on reducer-id:
 * - (start from 0) one reducer for each dictionary column, UHC may have more than one reducer
 * - one reducer to get min/max of date partition column
 * - (at the end) one or more reducers to collect row counts for cuboids using HLL
 */
public class FactDistinctColumnsReducerMapping {

    public static final int MARK_FOR_PARTITION_COL = -2;
    public static final int MARK_FOR_HLL_COUNTER = -1;

    final private int nDictValueCollectors;
    final private int datePartitionReducerId;
    final private int nCuboidRowCounters;
    final private int nTotalReducers;

    final private List<TblColRef> allDictCols;
    final private int[] dictColIdToReducerBeginId;
    final private int[] reducerRolePlay; // >=0 for dict col id, <0 for partition col and hll counter (using markers)

    public FactDistinctColumnsReducerMapping(CubeInstance cube) {
        this(cube, 0);
    }

    public FactDistinctColumnsReducerMapping(CubeInstance cube, int cuboidRowCounterReducerNum) {
        CubeDesc desc = cube.getDescriptor();

        allDictCols = new ArrayList(desc.getAllColumnsNeedDictionaryBuilt());

        dictColIdToReducerBeginId = new int[allDictCols.size() + 1];

        int uhcReducerCount = cube.getConfig().getUHCReducerCount();
        List<TblColRef> uhcList = desc.getAllUHCColumns();
        int counter = 0;
        for (int i = 0; i < allDictCols.size(); i++) {
            dictColIdToReducerBeginId[i] = counter;
            boolean isUHC = uhcList.contains(allDictCols.get(i));
            counter += (isUHC) ? uhcReducerCount : 1;
        }

        dictColIdToReducerBeginId[allDictCols.size()] = counter;
        nDictValueCollectors = counter;
        datePartitionReducerId = counter;

        nCuboidRowCounters = cuboidRowCounterReducerNum == 0 ? //
                MapReduceUtil.getCuboidHLLCounterReducerNum(cube) : cuboidRowCounterReducerNum;
        nTotalReducers = nDictValueCollectors + 1 + nCuboidRowCounters;

        reducerRolePlay = new int[nTotalReducers];
        for (int i = 0, dictId = 0; i < nTotalReducers; i++) {
            if (i > datePartitionReducerId) {
                // cuboid HLL counter reducer
                reducerRolePlay[i] = MARK_FOR_HLL_COUNTER;
            } else if (i == datePartitionReducerId) {
                // date partition min/max reducer
                reducerRolePlay[i] = MARK_FOR_PARTITION_COL;
            } else {
                // dict value collector reducer
                if (i == dictColIdToReducerBeginId[dictId + 1])
                    dictId++;

                reducerRolePlay[i] = dictId;
            }
        }
    }
    
    public List<TblColRef> getAllDictCols() {
        return allDictCols;
    }
    
    public int getTotalReducerNum() {
        return nTotalReducers;
    }
    
    public int getCuboidRowCounterReducerNum() {
        return nCuboidRowCounters;
    }

    public int getReducerIdForDictCol(int dictColId, Object fieldValue) {
        int begin = dictColIdToReducerBeginId[dictColId];
        int span = dictColIdToReducerBeginId[dictColId + 1] - begin;
        
        if (span == 1)
            return begin;
        
        int hash = fieldValue == null ? 0 : fieldValue.hashCode();
        return begin + Math.abs(hash) % span;
    }
    
    public int[] getAllRolePlaysForReducers() {
        return reducerRolePlay;
    }

    public int getRolePlayOfReducer(int reducerId) {
        return reducerRolePlay[reducerId];
    }
    
    public boolean isCuboidRowCounterReducer(int reducerId) {
        return getRolePlayOfReducer(reducerId) == MARK_FOR_HLL_COUNTER;
    }
    
    public boolean isPartitionColReducer(int reducerId) {
        return getRolePlayOfReducer(reducerId) == MARK_FOR_PARTITION_COL;
    }

    public TblColRef getDictColForReducer(int reducerId) {
        int role = getRolePlayOfReducer(reducerId);
        if (role < 0)
            throw new IllegalStateException();
        
        return allDictCols.get(role);
    }

    public int getReducerNumForDictCol(TblColRef col) {
        int dictColId = allDictCols.indexOf(col);
        return dictColIdToReducerBeginId[dictColId + 1] - dictColIdToReducerBeginId[dictColId];
    }

    public int getReducerIdForDatePartitionColumn() {
        return datePartitionReducerId;
    }

    public int getReducerIdForCuboidRowCount(long cuboidId) {
        int rowCounterId = (int) (Math.abs(cuboidId) % nCuboidRowCounters);
        return datePartitionReducerId + 1 + rowCounterId;
    }
    
}
