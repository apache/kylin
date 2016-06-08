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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * @author yangli9
 * 
 */
@SuppressWarnings({ "rawtypes" })
public class MeasureDecoder {

    int nMeasures;
    DataTypeSerializer[] serializers;

    public MeasureDecoder(Collection<MeasureDesc> measureDescs) {
        this((MeasureDesc[]) measureDescs.toArray(new MeasureDesc[measureDescs.size()]));
    }

    public MeasureDecoder(MeasureDesc... measureDescs) {
        String[] dataTypes = new String[measureDescs.length];
        for (int i = 0; i < dataTypes.length; i++) {
            dataTypes[i] = measureDescs[i].getFunction().getReturnType();
        }
        init(dataTypes);
    }

    public MeasureDecoder(DataType... dataTypes) {
        init(dataTypes);
    }

    public MeasureDecoder(String... dataTypes) {
        init(dataTypes);
    }

    private void init(String[] dataTypes) {
        DataType[] typeInstances = new DataType[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            typeInstances[i] = DataType.getType(dataTypes[i]);
        }
        init(typeInstances);
    }

    private void init(DataType[] dataTypes) {
        nMeasures = dataTypes.length;
        serializers = new DataTypeSerializer[nMeasures];

        for (int i = 0; i < nMeasures; i++) {
            serializers[i] = DataTypeSerializer.create(dataTypes[i]);
        }
    }

    public DataTypeSerializer getSerializer(int idx) {
        return serializers[idx];
    }

    public int[] getPeekLength(ByteBuffer buf) {
        int[] length = new int[nMeasures];
        int offset = 0;
        for (int i = 0; i < nMeasures; i++) {
            length[i] = serializers[i].peekLength(buf);
            offset += length[i];
            buf.position(offset);
        }
        return length;
    }

    public void decode(ByteBuffer buf, Object[] result) {
        assert result.length == nMeasures;
        for (int i = 0; i < nMeasures; i++) {
            result[i] = serializers[i].deserialize(buf);
        }
    }

}
