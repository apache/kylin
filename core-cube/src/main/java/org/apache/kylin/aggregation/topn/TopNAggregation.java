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

package org.apache.kylin.aggregation.topn;

import java.util.List;

import org.apache.kylin.aggregation.AggregationType;
import org.apache.kylin.aggregation.MeasureAggregator;
import org.apache.kylin.aggregation.hllc.HLLCSerializer;
import org.apache.kylin.common.datatype.DataType;
import org.apache.kylin.common.datatype.DataTypeSerializer;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class TopNAggregation extends AggregationType {

    private final DataType dataType;

    public TopNAggregation(String dataType) {
        this.dataType = DataType.getType(dataType);
        
        if (this.dataType.getPrecision() < 1 || this.dataType.getPrecision() > 1000)
            throw new IllegalArgumentException("TopN precision must be between 1 and 1000");
    }

    @Override
    public DataType getAggregationDataType() {
        return dataType;
    }

    @Override
    public Class<? extends DataTypeSerializer<?>> getAggregationDataSeralizer() {
        return HLLCSerializer.class;
    }
    
    @Override
    public void validate(MeasureDesc measureDesc) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public MeasureAggregator<?> newAggregator() {
        return new TopNAggregator();
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(MeasureDesc measureDesc) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object reEncodeDictionary(Object value, List<Dictionary<?>> oldDicts, List<Dictionary<?>> newDicts) {
        // TODO Auto-generated method stub
        return null;
    }

}
