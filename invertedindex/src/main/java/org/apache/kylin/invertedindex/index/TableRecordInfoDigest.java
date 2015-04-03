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

package org.apache.kylin.invertedindex.index;

import com.google.common.base.Objects;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.DataType;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by honma on 11/10/14.
 */
public class TableRecordInfoDigest {

    private String[] metricDataTypes;
    private int nColumns;
	private int byteFormLen;

	private int[] offsets;// column offset in byte form row
	private int[] dictMaxIds;// max id for each of the dict
	private int[] lengths;// length of each encoded dict
	private boolean[] isMetric;// whether it's metric or dict
    private FixedLenMeasureCodec[] measureCodecs;

    public TableRecordInfoDigest(int nColumns, int byteFormLen, int[] offsets,
			int[] dictMaxIds, int[] lengths, boolean[] isMetric,
			String[] metricDataTypes) {
		this.nColumns = nColumns;
		this.byteFormLen = byteFormLen;
		this.offsets = offsets;
		this.dictMaxIds = dictMaxIds;
		this.lengths = lengths;
		this.isMetric = isMetric;
        this.metricDataTypes = metricDataTypes;
        this.measureCodecs = new FixedLenMeasureCodec[nColumns];
        for (int i = 0; i < isMetric.length; i++) {
            if (isMetric[i]) {
                measureCodecs[i] = FixedLenMeasureCodec.get(DataType.getInstance(metricDataTypes[i]));
            }
        }
    }

	private TableRecordInfoDigest() {
	}

	public int getByteFormLen() {
		return byteFormLen;
	}

	public boolean isMetrics(int col) {
		return isMetric[col];
	}

    public boolean[] isMetrics() {
        return isMetric;
    }

	public int getColumnCount() {
		return nColumns;
	}

	public int offset(int col) {
		return offsets[col];
	}

	public int length(int col) {
		return lengths[col];
	}

	public int getMaxID(int col) {
		return dictMaxIds[col];
	}

    public boolean[] getIsMetric() {
        return isMetric;
    }

    public String[] getMetricDataTypes() {
        return metricDataTypes;
    }

    public RawTableRecord createTableRecordBytes() {
		return new RawTableRecord(this);
	}

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(nColumns, offsets, dictMaxIds, lengths, isMetric, metricDataTypes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof TableRecordInfoDigest) {
            TableRecordInfoDigest other = (TableRecordInfoDigest) obj;
            return Objects.equal(this.nColumns, other.nColumns) &&
                    Arrays.equals(this.offsets, other.offsets) &&
                    Arrays.equals(this.dictMaxIds, other.dictMaxIds) &&
                    Arrays.equals(this.lengths, other.lengths) &&
                    Arrays.equals(this.isMetric, other.isMetric) &&
                    Arrays.equals(this.metricDataTypes, other.metricDataTypes);
        } else {
            return false;
        }
    }

    // metrics go with fixed-len codec
	@SuppressWarnings("unchecked")
	public FixedLenMeasureCodec<LongWritable> codec(int col) {
		// yes, all metrics are long currently
        return measureCodecs[col];
	}

	public static byte[] serialize(TableRecordInfoDigest o) {
		ByteBuffer buf = ByteBuffer.allocate(Serializer.SERIALIZE_BUFFER_SIZE);
		serializer.serialize(o, buf);
		byte[] result = new byte[buf.position()];
		System.arraycopy(buf.array(), 0, result, 0, buf.position());
		return result;
	}

	public static TableRecordInfoDigest deserialize(byte[] bytes) {
		return serializer.deserialize(ByteBuffer.wrap(bytes));
	}

	public static TableRecordInfoDigest deserialize(ByteBuffer buffer) {
		return serializer.deserialize(buffer);
	}

	private static final Serializer serializer = new Serializer();

	private static class Serializer implements
			BytesSerializer<TableRecordInfoDigest> {

		@Override
		public void serialize(TableRecordInfoDigest value, ByteBuffer out) {
			BytesUtil.writeVInt(value.nColumns, out);
			BytesUtil.writeVInt(value.byteFormLen, out);
			BytesUtil.writeIntArray(value.offsets, out);
			BytesUtil.writeIntArray(value.dictMaxIds, out);
			BytesUtil.writeIntArray(value.lengths, out);
			BytesUtil.writeBooleanArray(value.isMetric, out);
            BytesUtil.writeAsciiStringArray(value.metricDataTypes, out);
		}

		@Override
		public TableRecordInfoDigest deserialize(ByteBuffer in) {
			TableRecordInfoDigest result = new TableRecordInfoDigest();
			result.nColumns = BytesUtil.readVInt(in);
			result.byteFormLen = BytesUtil.readVInt(in);
			result.offsets = BytesUtil.readIntArray(in);
			result.dictMaxIds = BytesUtil.readIntArray(in);
			result.lengths = BytesUtil.readIntArray(in);
			result.isMetric = BytesUtil.readBooleanArray(in);
            result.metricDataTypes = BytesUtil.readAsciiStringArray(in);
			return result;
		}

	}
}
