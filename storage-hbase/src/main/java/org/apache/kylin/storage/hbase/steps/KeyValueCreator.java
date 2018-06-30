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

package org.apache.kylin.storage.hbase.steps;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;

/**
 * @author George Song (ysong1)
 */
public class KeyValueCreator implements Serializable {
    byte[] cfBytes;
    byte[] qBytes;
    long timestamp;

    int[] refIndex;
    MeasureDesc[] refMeasures;

    BufferedMeasureCodec codec;
    Object[] colValues;

    public boolean isFullCopy;

    public KeyValueCreator(CubeDesc cubeDesc, HBaseColumnDesc colDesc) {

        cfBytes = Bytes.toBytes(colDesc.getColumnFamilyName());
        qBytes = Bytes.toBytes(colDesc.getQualifier());
        timestamp = 0; // use 0 for timestamp

        refIndex = colDesc.getMeasureIndex();
        refMeasures = colDesc.getMeasures();

        codec = new BufferedMeasureCodec(refMeasures);
        colValues = new Object[refMeasures.length];

        isFullCopy = true;
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        for (int i = 0; i < measures.size(); i++) {
            if (refIndex.length <= i || refIndex[i] != i)
                isFullCopy = false;
        }
    }

    public KeyValue create(Text key, Object[] measureValues) {
        return create(key.getBytes(), 0, key.getLength(), measureValues);
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, Object[] measureValues) {
        for (int i = 0; i < colValues.length; i++) {
            colValues[i] = measureValues[refIndex[i]];
        }

        ByteBuffer valueBuf = codec.encode(colValues);

        return create(keyBytes, keyOffset, keyLength, valueBuf.array(), 0, valueBuf.position());
    }

    public KeyValue create(byte[] keyBytes, int keyOffset, int keyLength, byte[] value, int voffset, int vlen) {
        return new KeyValue(keyBytes, keyOffset, keyLength, //
                cfBytes, 0, cfBytes.length, //
                qBytes, 0, qBytes.length, //
                timestamp, KeyValue.Type.Put, //
                value, voffset, vlen);
    }

    public KeyValue create(Text key, byte[] value, int voffset, int vlen) {
        return create(key.getBytes(), 0, key.getLength(), value, voffset, vlen);
    }

}
