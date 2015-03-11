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

package org.apache.kylin.invertedindex.model;

import java.util.*;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.index.*;

/**
 * @author yangli9
 */
public class IIKeyValueCodec {

	public static final int SHARD_LEN = 2;
	public static final int TIMEPART_LEN = 8;
	public static final int COLNO_LEN = 2;

	private TableRecordInfoDigest infoDigest;

	public IIKeyValueCodec(TableRecordInfoDigest digest) {
		this.infoDigest = digest;
	}

	public Collection<KeyValuePair> encodeKeyValue(Slice slice) {
		ArrayList<KeyValuePair> result = Lists
				.newArrayList();
		ColumnValueContainer[] containers = slice.getColumnValueContainers();
		for (int col = 0; col < containers.length; col++) {
			if (containers[col] instanceof BitMapContainer) {
				result.addAll(collectKeyValues(slice, col, (BitMapContainer) containers[col]));
			} else if (containers[col] instanceof CompressedValueContainer) {
				result.add(collectKeyValues(slice, col, (CompressedValueContainer) containers[col]));
			} else {
				throw new IllegalArgumentException("Unknown container class "
						+ containers[col].getClass());
			}
		}
		return result;
	}

	private KeyValuePair collectKeyValues(Slice slice,
			int col,
			CompressedValueContainer container) {
		ImmutableBytesWritable key = encodeKey(slice.getShard(),
				slice.getTimestamp(), col, -1);
		ImmutableBytesWritable value = container.toBytes();
        return new KeyValuePair(col, key, value);
	}

	private List<KeyValuePair> collectKeyValues(Slice slice,
			int col,
			BitMapContainer container) {
		List<ImmutableBytesWritable> values = container.toBytes();
        ArrayList<KeyValuePair> list = Lists.newArrayListWithExpectedSize(values.size());
		for (int v = 0; v < values.size(); v++) {
			ImmutableBytesWritable key = encodeKey(slice.getShard(),
					slice.getTimestamp(), col, v);
            list.add(new KeyValuePair(col, key, values.get(v)));
		}
        return list;
	}

	ImmutableBytesWritable encodeKey(short shard, long timestamp, int col,
			int colValue) {
		byte[] bytes = new byte[20];
		int len = encodeKey(shard, timestamp, col, colValue, bytes, 0);
		return new ImmutableBytesWritable(bytes, 0, len);
	}

	int encodeKey(short shard, long timestamp, int col, int colValue,
			byte[] buf, int offset) {
		int i = offset;

		BytesUtil.writeUnsigned(shard, buf, i, SHARD_LEN);
		i += SHARD_LEN;
		BytesUtil.writeLong(timestamp, buf, i, TIMEPART_LEN);
		i += TIMEPART_LEN;

		BytesUtil.writeUnsigned(col, buf, i, COLNO_LEN);
		i += COLNO_LEN;

		if (colValue >= 0) {
			int colLen = infoDigest.length(col);
			BytesUtil.writeUnsigned(colValue, buf, i, colLen);
			i += colLen;
		}

		return i - offset;
	}

	public Iterable<Slice> decodeKeyValue(
			Iterable<KeyValuePair> kvs) {
		return new Decoder(infoDigest, kvs);
	}

	private static class Decoder implements Iterable<Slice> {

		TableRecordInfoDigest info;
		Iterator<KeyValuePair> iterator;

		Slice next = null;
		short curShard = Short.MIN_VALUE;
		long curSliceTimestamp = Long.MIN_VALUE;
		int curCol = -1;
		int curColValue = -1;
		short lastShard = Short.MIN_VALUE;
		long lastSliceTimestamp = Long.MIN_VALUE;
		int lastCol = -1;
		ColumnValueContainer[] containers = null;
		List<ImmutableBytesWritable> bitMapValues = Lists.newArrayList();
        Map<Integer, Dictionary<?>> localDictionaries = Maps.newHashMap();

		Decoder(TableRecordInfoDigest info,
				Iterable<KeyValuePair> kvs) {
			this.info = info;
			this.iterator = kvs.iterator();
		}

		private void goToNext() {
			if (next != null) { // was not fetched
				return;
			}

			// NOTE the input keys are ordered
			while (next == null && iterator.hasNext()) {
                KeyValuePair kv = iterator.next();
				ImmutableBytesWritable k = kv.getKey();
				ImmutableBytesWritable v = kv.getValue();
				decodeKey(k);
                localDictionaries.put(curCol, kv.getDictionary());

				if (curShard != lastShard
						|| curSliceTimestamp != lastSliceTimestamp) {
					makeNext();
				}
				consumeCurrent(v);
			}
			if (next == null) {
				makeNext();
			}
		}

		private void decodeKey(ImmutableBytesWritable k) {
			byte[] buf = k.get();
			int i = k.getOffset();

			curShard = (short) BytesUtil.readUnsigned(buf, i, SHARD_LEN);
			i += SHARD_LEN;
			curSliceTimestamp = BytesUtil.readLong(buf, i, TIMEPART_LEN);
			i += TIMEPART_LEN;

			curCol = BytesUtil.readUnsigned(buf, i, COLNO_LEN);
			i += COLNO_LEN;

			if (i - k.getOffset() < k.getLength()) {
				// bitmap
				int colLen = info.length(curCol);
				curColValue = BytesUtil.readUnsigned(buf, i, colLen);
				i += colLen;
			} else {
				// value list
				curColValue = -1;
			}
		}

		private void consumeCurrent(ImmutableBytesWritable v) {
			if (curCol != lastCol && bitMapValues.size() > 0) { // end of a
																// bitmap
																// container
				addBitMapContainer(lastCol);
			}
			if (curColValue < 0) {
				CompressedValueContainer c = new CompressedValueContainer(info,
						curCol, 0);
				c.fromBytes(v);
				addContainer(curCol, c);
			} else {
				assert curColValue == bitMapValues.size();
				// make a copy, the value object from caller is typically reused
				// through iteration
				bitMapValues.add(new ImmutableBytesWritable(v));
			}

			lastShard = curShard;
			lastSliceTimestamp = curSliceTimestamp;
			lastCol = curCol;
		}

		private void makeNext() {
			if (bitMapValues.isEmpty() == false) {
				addBitMapContainer(lastCol);
			}
			if (containers != null) {
				next = new Slice(info, lastShard, lastSliceTimestamp,
						containers);
                next.setLocalDictionaries(Maps.newHashMap(localDictionaries));
			}
			lastSliceTimestamp = Long.MIN_VALUE;
			lastCol = -1;
			containers = null;
			bitMapValues.clear();
            localDictionaries.clear();
		}

		private void addBitMapContainer(int col) {
			BitMapContainer c = new BitMapContainer(info, col);
			c.fromBytes(bitMapValues);
			addContainer(col, c);
			bitMapValues.clear();
		}

		private void addContainer(int col, ColumnValueContainer c) {
			if (containers == null) {
				containers = new ColumnValueContainer[info.getColumnCount()];
			}
			containers[col] = c;
		}

		@Override
		public Iterator<Slice> iterator() {
			return new Iterator<Slice>() {
				@Override
				public boolean hasNext() {
					goToNext();
					return next != null;
				}

				@Override
				public Slice next() {
					Slice result = next;
					next = null;
					return result;
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}

	}

}
