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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

@SuppressWarnings({ "rawtypes", "unchecked" })
/**
 * This is just for example and is INCORRECT when numbers are encoded to bytes and compared in filter.
 * 
 * A correct implementation must ensure dimension values preserve order after encoded, e.g. by using an
 * order preserving dictionary.
 * 
 * @author yangli9
 */
public class GTSampleCodeSystem implements IGTCodeSystem {

    private GTInfo info;
    private DataTypeSerializer[] serializers;
    private IGTComparator comparator;

    public GTSampleCodeSystem() {
    }

    @Override
    public void init(GTInfo info) {
        this.info = info;

        this.serializers = new DataTypeSerializer[info.getColumnCount()];
        for (int i = 0; i < info.getColumnCount(); i++) {
            this.serializers[i] = DataTypeSerializer.create(info.colTypes[i]);
        }

        this.comparator = new DefaultGTComparator();
    }

    @Override
    public int codeLength(int col, ByteBuffer buf) {
        return serializers[col].peekLength(buf);
    }

    @Override
    public int maxCodeLength(int col) {
        return serializers[col].maxLength();
    }

    @Override
    public DimensionEncoding getDimEnc(int col) {
        return null;
    }

    @Override
    public IGTComparator getComparator() {
        return comparator;
    }

    // ============================================================================

    @Override
    public MeasureAggregator<?>[] newMetricsAggregators(ImmutableBitSet columns, String[] aggrFunctions) {
        assert columns.trueBitCount() == aggrFunctions.length;

        MeasureAggregator<?>[] result = new MeasureAggregator[aggrFunctions.length];
        for (int i = 0; i < result.length; i++) {
            int col = columns.trueBitAt(i);
            result[i] = MeasureAggregator.create(aggrFunctions[i], info.getColumnType(col));
        }
        return result;
    }

    @Override
    public void encodeColumnValue(int col, Object value, ByteBuffer buf) {
        serializers[col].serialize(value, buf);
    }

    @Override
    public void encodeColumnValue(int col, Object value, int roundingFlag, ByteBuffer buf) {
        // ignore rounding
        encodeColumnValue(col, value, buf);
    }

    @Override
    public Object decodeColumnValue(int col, ByteBuffer buf) {
        return serializers[col].deserialize(buf);
    }

    @SuppressWarnings("unused") //used by reflection
    public static final BytesSerializer<IGTCodeSystem> serializer = new BytesSerializer<IGTCodeSystem>() {
        @Override
        public void serialize(IGTCodeSystem value, ByteBuffer out) {
        }

        @Override
        public IGTCodeSystem deserialize(ByteBuffer in) {
            return new GTSampleCodeSystem();
        }
    };

}
