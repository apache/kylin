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

package org.apache.kylin.measure.basic;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.BigDecimalSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.datatype.DoubleSerializer;
import org.apache.kylin.metadata.datatype.LongSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;

public class BasicMeasureType extends MeasureType {
    
    private final String funcName;
    private final DataType dataType;

    public BasicMeasureType(String funcName, DataType dataType) {
        this.funcName = funcName;
        this.dataType = dataType;
    }

    @Override
    public DataType getAggregationDataType() {
        return dataType;
    }

    public Class<? extends DataTypeSerializer<?>> getAggregationDataSeralizer() {
        if (dataType.isIntegerFamily())
            return LongSerializer.class;
        else if (dataType.isDecimal())
            return BigDecimalSerializer.class;
        else if (dataType.isNumberFamily())
            return DoubleSerializer.class;
        else
            throw new IllegalArgumentException("No serializer for aggregation type " + dataType);
    }
    
    @Override
    public void validate(MeasureDesc measureDesc) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public MeasureIngester<?> newIngester() {
        if (dataType.isIntegerFamily())
            return new LongIngester();
        else if (dataType.isDecimal())
            return new BigDecimalIngester();
        else if (dataType.isNumberFamily())
            return new DoubleIngester();
        else
            throw new IllegalArgumentException("No ingester for aggregation type " + dataType);
    }
    
    @Override
    public MeasureAggregator<?> newAggregator() {
        if (isSum() || isCount()) {
            if (dataType.isDecimal())
                return new BigDecimalSumAggregator();
            else if (dataType.isIntegerFamily())
                return new LongSumAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleSumAggregator();
        } else if (isMax()) {
            if (dataType.isDecimal())
                return new BigDecimalMaxAggregator();
            else if (dataType.isIntegerFamily())
                return new LongMaxAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleMaxAggregator();
        } else if (isMin()) {
            if (dataType.isDecimal())
                return new BigDecimalMinAggregator();
            else if (dataType.isIntegerFamily())
                return new LongMinAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleMinAggregator();
        }
        throw new IllegalArgumentException("No aggregator for func '" + funcName + "' and return type '" + dataType + "'");
    }
    
    private boolean isSum() {
        return FunctionDesc.FUNC_SUM.equalsIgnoreCase(funcName);
    }

    private boolean isCount() {
        return FunctionDesc.FUNC_COUNT.equalsIgnoreCase(funcName);
    }
    
    private boolean isMax() {
        return FunctionDesc.FUNC_MAX.equalsIgnoreCase(funcName);
    }
    
    private boolean isMin() {
        return FunctionDesc.FUNC_MIN.equalsIgnoreCase(funcName);
    }
    
}
