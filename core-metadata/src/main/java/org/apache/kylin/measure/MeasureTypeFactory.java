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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

abstract public class MeasureTypeFactory<T> {

    abstract public MeasureType<T> createMeasureType(String funcName, DataType dataType);
    
    abstract public String getAggrFunctionName();
    abstract public String getAggrDataTypeName();
    abstract public Class<? extends DataTypeSerializer<T>> getAggrDataTypeSerializer();
    
    // ============================================================================
    
    private static Map<String, MeasureTypeFactory<?>> factories = Maps.newHashMap();
    private static MeasureTypeFactory<?> defaultFactory = new BasicMeasureType.Factory();
    
    public static synchronized void init(KylinConfig config) {
        if (factories.isEmpty() == false)
            return;
        
        List<MeasureTypeFactory<?>> factoryInsts = Lists.newArrayList();
        
        // two built-in advanced measure types
        factoryInsts.add(new HLLCMeasureType.Factory());
        factoryInsts.add(new TopNMeasureType.Factory());
        
        // more custom measure types
        for (String factoryClz : config.getMeasureTypeFactories()) {
            factoryInsts.add((MeasureTypeFactory<?>) ClassUtil.newInstance(factoryClz));
        }
        
        // register factories & data type serializers
        for (MeasureTypeFactory<?> factory : factoryInsts) {
            String funcName = factory.getAggrFunctionName().toUpperCase();
            String dataTypeName = factory.getAggrDataTypeName().toLowerCase();
            Class<? extends DataTypeSerializer<?>> serializer = factory.getAggrDataTypeSerializer();
            
            DataType.register(dataTypeName);
            DataTypeSerializer.register(dataTypeName, serializer);
            factories.put(funcName, factory);
        }
    }
    
    public static MeasureType<?> create(String funcName, String dataType) {
        return create(funcName, DataType.getType(dataType));
    }
    
    public static MeasureType<?> create(String funcName, DataType dataType) {
        funcName = funcName.toUpperCase();
        
        MeasureTypeFactory<?> factory = factories.get(funcName);
        if (factory == null)
            factory = defaultFactory;
        
        return factory.createMeasureType(funcName, dataType);
    }
}
