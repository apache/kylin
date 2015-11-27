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

import java.util.List;
import java.util.Map;

import org.apache.kylin.measure.basic.BasicMeasureFactory;
import org.apache.kylin.measure.hllc.HLLCAggregationFactory;
import org.apache.kylin.measure.topn.TopNMeasureFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

abstract public class MeasureType {
    
    private static final Map<String, IMeasureFactory> factoryRegistry = Maps.newConcurrentMap();
    private static final IMeasureFactory defaultFactory = new BasicMeasureFactory();
    
    static {
        factoryRegistry.put(FunctionDesc.FUNC_COUNT_DISTINCT, new HLLCAggregationFactory());
        factoryRegistry.put(FunctionDesc.FUNC_TOP_N, new TopNMeasureFactory());
    }
    
    public static MeasureType create(FunctionDesc function) {
        return create(function.getExpression(), function.getReturnType());
    }

    public static MeasureType create(String funcName, String dataType) {
        funcName = funcName.toUpperCase();
        dataType = dataType.toLowerCase();
        
        IMeasureFactory factory = factoryRegistry.get(funcName);
        if (factory == null)
            factory = defaultFactory;
        
        MeasureType result = factory.createMeasureType(funcName, dataType);
        
        // register serializer for aggr data type
        DataType aggregationDataType = result.getAggregationDataType();
        if (DataTypeSerializer.hasRegistered(aggregationDataType.getName()) == false) {
            DataTypeSerializer.register(aggregationDataType.getName(), result.getAggregationDataSeralizer());
        }
        
        return result;
    }
    
    /* ============================================================================
     * Define
     * ---------------------------------------------------------------------------- */
    
    abstract public DataType getAggregationDataType();
    
    abstract public Class<? extends DataTypeSerializer<?>> getAggregationDataSeralizer();
    
    abstract public void validate(MeasureDesc measureDesc) throws IllegalArgumentException;
    
    /* ============================================================================
     * Build
     * ---------------------------------------------------------------------------- */
    
    abstract public MeasureIngester<?> newIngester();
    
    abstract public MeasureAggregator<?> newAggregator();
 
    abstract public List<TblColRef> getColumnsNeedDictionary(MeasureDesc measureDesc);
    
    /* ============================================================================
     * Cube Selection
     * ---------------------------------------------------------------------------- */
    
    /* ============================================================================
     * Query
     * ---------------------------------------------------------------------------- */
    
    /* ============================================================================
     * Storage
     * ---------------------------------------------------------------------------- */
    
}
