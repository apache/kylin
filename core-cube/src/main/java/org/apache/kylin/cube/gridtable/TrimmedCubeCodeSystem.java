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

package org.apache.kylin.cube.gridtable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

import com.google.common.collect.Maps;

/**
 * A limited code system which trims DictionaryDimEnc to TrimmedDimEnc (to avoid pushing down the useless dictionary)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TrimmedCubeCodeSystem extends CubeCodeSystem {

    public TrimmedCubeCodeSystem(DimensionEncoding[] dimEncs, Map<Integer, Integer> dependentMetricsMap) {
        super(dimEncs, dependentMetricsMap);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        DataTypeSerializer serializer = serializers[col];
        serializer.serialize(value, buf);
    }

    private static void writeDimensionEncoding(DimensionEncoding encoding, ByteBuffer out) {
        try {
            if (encoding == null) {
                BytesUtil.writeVInt(1, out);
            } else {
                BytesUtil.writeVInt(0, out);

                if (encoding instanceof DictionaryDimEnc) {
                    encoding = new TrimmedDimEnc(encoding.getLengthOfEncoding());
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(encoding);
                BytesUtil.writeByteArray(baos.toByteArray(), out);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DimensionEncoding readDimensionEncoding(ByteBuffer in) {
        try {
            int isNull = BytesUtil.readVInt(in);
            if (isNull == 1) {
                return null;
            }

            byte[] bytes = BytesUtil.readByteArray(in);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            DimensionEncoding ret = (DimensionEncoding) ois.readObject();
            return ret;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static final BytesSerializer<IGTCodeSystem> serializer = new BytesSerializer<IGTCodeSystem>() {
        @Override
        public void serialize(IGTCodeSystem ivalue, ByteBuffer out) {
            TrimmedCubeCodeSystem value = (TrimmedCubeCodeSystem) ivalue;
            BytesUtil.writeVInt(value.dependentMetricsMap.size(), out);
            for (Map.Entry<Integer, Integer> x : value.dependentMetricsMap.entrySet()) {
                BytesUtil.writeVInt(x.getKey(), out);
                BytesUtil.writeVInt(x.getValue(), out);
            }

            BytesUtil.writeVInt(value.dimEncs.length, out);
            for (int i = 0; i < value.dimEncs.length; i++) {
                DimensionEncoding enc = value.dimEncs[i];

                writeDimensionEncoding(enc, out);
            }
        }

        @Override
        public IGTCodeSystem deserialize(ByteBuffer in) {
            Map<Integer, Integer> dependentMetricsMap = Maps.newHashMap();

            int size = BytesUtil.readVInt(in);
            for (int i = 0; i < size; ++i) {
                int key = BytesUtil.readVInt(in);
                int value = BytesUtil.readVInt(in);
                dependentMetricsMap.put(key, value);
            }

            DimensionEncoding[] dimEncs = new DimensionEncoding[BytesUtil.readVInt(in)];
            for (int i = 0; i < dimEncs.length; i++) {
                dimEncs[i] = readDimensionEncoding(in);
            }

            return new TrimmedCubeCodeSystem(dimEncs, dependentMetricsMap);
        }
    };

}
