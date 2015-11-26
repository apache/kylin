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

package org.apache.kylin.measure.topn;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

public class TopNMeasureType extends MeasureType {

    private final DataType dataType;

    public TopNMeasureType(DataType dataType) {
        if ("topn".equals(dataType.getName()) == false)
            throw new IllegalArgumentException();
        
        this.dataType = dataType;
        
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

    @SuppressWarnings("rawtypes")
    @Override
    public MeasureIngester<?> newIngester() {
        return new MeasureIngester<TopNCounter>() {
            @Override
            public TopNCounter valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
                if (values.length != 2)
                    throw new IllegalArgumentException();
                
                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);
                String literal = values[1];
                
                // encode literal using dictionary
                TblColRef literalCol = measureDesc.getFunction().getTopNLiteralColumn();
                Dictionary<String> dictionary = dictionaryMap.get(literalCol);
                int keyEncodedValue = dictionary.getIdFromValue(literal);

                ByteArray key = new ByteArray(dictionary.getSizeOfId());
                BytesUtil.writeUnsigned(keyEncodedValue, key.array(), 0, dictionary.getSizeOfId());

                TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(dataType.getPrecision() * TopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);
                return topNCounter;
            }
        };
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
