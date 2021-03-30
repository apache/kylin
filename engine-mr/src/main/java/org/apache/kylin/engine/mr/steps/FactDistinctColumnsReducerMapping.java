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

import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Reducers play different roles based on reducer-id:
 * - (start from 0) one reducer for each dimension column, dictionary column, UHC may have more than one reducer
 * - (at the end) one or more reducers to collect row counts for cuboids using HLL
 */
public class FactDistinctColumnsReducerMapping {

    public static final int MARK_FOR_HLL_COUNTER = -1;

    final private int nCuboidRowCounters;
    final private int nDimReducers;
    final private int nTotalReducers;

    final private List<TblColRef> allDimDictCols = Lists.newArrayList();
    final private int[] colIdToReducerBeginId;
    final private int[] reducerRolePlay; // >=0 for dict col id, <0 for partition col and hll counter (using markers)

    public FactDistinctColumnsReducerMapping(CubeInstance cube) {
        this(cube, 0);
    }

    private FactDistinctColumnsReducerMapping(CubeInstance cube, int cuboidRowCounterReducerNum) {
        CubeDesc desc = cube.getDescriptor();
        Set<TblColRef> allCols = cube.getAllColumns();
        Set<TblColRef> dictCols = desc.getAllColumnsNeedDictionaryBuilt();
        List<TblColRef> dimCols = desc.listDimensionColumnsExcludingDerived(true);
        for (TblColRef colRef : allCols) {
            if (dictCols.contains(colRef)) {
                allDimDictCols.add(colRef);
            } else if (dimCols.indexOf(colRef) >= 0){
                allDimDictCols.add(colRef);
            }
        }

        colIdToReducerBeginId = new int[allDimDictCols.size() + 1];

        int uhcReducerCount = cube.getConfig().getUHCReducerCount();
        List<TblColRef> uhcList = desc.getAllUHCColumns();
        int counter = 0;
        for (int i = 0; i < allDimDictCols.size(); i++) {
            colIdToReducerBeginId[i] = counter;
            boolean isUHC = uhcList.contains(allDimDictCols.get(i));
            counter += (isUHC) ? uhcReducerCount : 1;
        }
        colIdToReducerBeginId[allDimDictCols.size()] = counter;
        nDimReducers = counter;

        nCuboidRowCounters = cuboidRowCounterReducerNum == 0 ? //
                MapReduceUtil.getCuboidHLLCounterReducerNum(cube) : cuboidRowCounterReducerNum;
        nTotalReducers = nDimReducers + nCuboidRowCounters;

        reducerRolePlay = new int[nTotalReducers];
        for (int i = 0, dictId = 0; i < nTotalReducers; i++) {
            if (i >= nDimReducers) {
                // cuboid HLL counter reducer
                reducerRolePlay[i] = MARK_FOR_HLL_COUNTER;
            } else {
                if (i == colIdToReducerBeginId[dictId + 1])
                    dictId++;

                reducerRolePlay[i] = dictId;
            }
        }
    }
    
    public List<TblColRef> getAllDimDictCols() {
        return allDimDictCols;
    }
    
    public int getTotalReducerNum() {
        return nTotalReducers;
    }
    
    public int getCuboidRowCounterReducerNum() {
        return nCuboidRowCounters;
    }

    public int getReducerIdForCol(int colId, Object fieldValue) {
        int begin = colIdToReducerBeginId[colId];
        int span = colIdToReducerBeginId[colId + 1] - begin;
        
        if (span == 1)
            return begin;
        
        int hash = fieldValue == null ? 0 : fieldValue.hashCode();
        return begin + Math.abs(hash % span);
    }
    
    public int[] getAllRolePlaysForReducers() {
        return reducerRolePlay;
    }

    public int getRolePlayOfReducer(int reducerId) {
        return reducerRolePlay[reducerId % nTotalReducers];
    }
    
    public boolean isCuboidRowCounterReducer(int reducerId) {
        return getRolePlayOfReducer(reducerId) == MARK_FOR_HLL_COUNTER;
    }

    public TblColRef getColForReducer(int reducerId) {
        int role = getRolePlayOfReducer(reducerId % nTotalReducers);
        if (role < 0)
            throw new IllegalStateException();
        
        return allDimDictCols.get(role);
    }

    public int getReducerNumForDimCol(TblColRef col) {
        int dictColId = allDimDictCols.indexOf(col);
        return colIdToReducerBeginId[dictColId + 1] - colIdToReducerBeginId[dictColId];
    }

    public int getReducerIdForCuboidRowCount(long cuboidId) {
        int rowCounterId = (int) (Math.abs(cuboidId) % nCuboidRowCounters);
        return nDimReducers + rowCounterId;
    }
    
}
