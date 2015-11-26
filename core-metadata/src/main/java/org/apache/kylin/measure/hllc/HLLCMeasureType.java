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

package org.apache.kylin.measure.hllc;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class HLLCMeasureType extends MeasureType {

    private final DataType dataType;

    public HLLCMeasureType(DataType dataType) {
        if ("hllc".equals(dataType.getName()) == false)
            throw new IllegalArgumentException();
        
        this.dataType = dataType;

        if (this.dataType.getPrecision() < 10 || this.dataType.getPrecision() > 16)
            throw new IllegalArgumentException("HLLC precision must be between 10 and 16");
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
    public MeasureIngester<?> newIngester() {
        return new MeasureIngester<HyperLogLogPlusCounter>() {
            HyperLogLogPlusCounter current = new HyperLogLogPlusCounter(dataType.getPrecision());

            @Override
            public HyperLogLogPlusCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                HyperLogLogPlusCounter hllc = current;
                hllc.clear();
                for (String v : values)
                    hllc.add(v == null ? "__nUlL__" : v);
                return hllc;
            }
        };
    }

    @Override
    public MeasureAggregator<?> newAggregator() {
        if (dataType.isHLLC())
            return new HLLCAggregator(dataType.getPrecision());
        else
            return new LDCAggregator();
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
