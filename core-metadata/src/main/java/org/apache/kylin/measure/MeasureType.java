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

package org.apache.kylin.measure;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

abstract public class MeasureType<T> {
    
    /* ============================================================================
     * Define
     * ---------------------------------------------------------------------------- */
    
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        return;
    }
    
    public boolean isMemoryHungry() {
        return false;
    }
    
    /* ============================================================================
     * Build
     * ---------------------------------------------------------------------------- */
    
    abstract public MeasureIngester<T> newIngester();
    
    abstract public MeasureAggregator<T> newAggregator();
 
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        return Collections.emptyList();
    }

    /* ============================================================================
     * Cube Selection
     * ---------------------------------------------------------------------------- */
    
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc measureDesc) {
        return null;
    }
    
    /* ============================================================================
     * Query
     * ---------------------------------------------------------------------------- */
    
    // TODO support user defined calcite aggr function
    
    abstract public boolean needRewrite();
    
    abstract public Class<?> getRewriteCalciteAggrFunctionClass();
    
    /* ============================================================================
     * Storage
     * ---------------------------------------------------------------------------- */
    
    public void beforeStorageQuery(MeasureDesc measureDesc, SQLDigest sqlDigest) {
    }
    
    public boolean needAdvancedTupleFilling() {
        return false;
    }

    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        tuple.setMeasureValue(indexInTuple, measureValue);
    }
    
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo returnTupleInfo, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        throw new UnsupportedOperationException();
    }
    
    public static interface IAdvMeasureFiller {
        
        void reload(Object measureValue);
        
        int getNumOfRows();
        
        void fillTuplle(Tuple tuple, int row);
    }
}
