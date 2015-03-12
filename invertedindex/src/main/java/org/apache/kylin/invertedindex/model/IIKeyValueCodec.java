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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.index.ColumnValueContainer;
import org.apache.kylin.invertedindex.index.CompressedValueContainer;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;

import java.util.*;

/**
 * @author yangli9
 */
public class IIKeyValueCodec implements KeyValueCodec {

	public static final int SHARD_LEN = 2;
	public static final int TIMEPART_LEN = 8;
	public static final int COLNO_LEN = 2;

	public IIKeyValueCodec() {
	}

    @Override
	public Collection<IIRow> encodeKeyValue(Slice slice) {
		ArrayList<IIRow> result = Lists
				.newArrayList();
		ColumnValueContainer[] containers = slice.getColumnValueContainers();
		for (int col = 0; col < containers.length; col++) {
			if (containers[col] instanceof CompressedValueContainer) {
				result.add(collectKeyValues(slice, col, (CompressedValueContainer) containers[col]));
			} else {
				throw new IllegalArgumentException("Unknown container class "
						+ containers[col].getClass());
			}
		}
		return result;
	}

	private IIRow collectKeyValues(Slice slice,
			int col,
			CompressedValueContainer container) {
		ImmutableBytesWritable key = encodeKey(slice.getShard(), slice.getTimestamp(), col);
		ImmutableBytesWritable value = container.toBytes();
        return new IIRow(key, value, null);
	}

	ImmutableBytesWritable encodeKey(short shard, long timestamp, int col) {
		byte[] bytes = new byte[20];
		int len = encodeKey(shard, timestamp, col, bytes, 0);
		return new ImmutableBytesWritable(bytes, 0, len);
	}

	int encodeKey(short shard, long timestamp, int col, byte[] buf, int offset) {
		int i = offset;

		BytesUtil.writeUnsigned(shard, buf, i, SHARD_LEN);
		i += SHARD_LEN;
		BytesUtil.writeLong(timestamp, buf, i, TIMEPART_LEN);
		i += TIMEPART_LEN;

		BytesUtil.writeUnsigned(col, buf, i, COLNO_LEN);
		i += COLNO_LEN;

		return i - offset;
	}

    @Override
	public Iterable<Slice> decodeKeyValue(Iterable<IIRow> kvs) {
		return new Decoder(kvs);
	}

    private static class SliceIterator implements Iterator<Slice> {

        private final Iterator<IIRow> rowIterator;

        public SliceIterator(Iterator<IIRow> rowIterator) {
            this.rowIterator = rowIterator;
        }

        @Override
        public boolean hasNext() {
            if (!rowIterator.hasNext()) {
                return false;
            }
            return false;
        }

        @Override
        public Slice next() {
            return null;
        }

        @Override
        public void remove() {

        }
    }

	private static class Decoder implements Iterable<Slice> {

		TableRecordInfoDigest info;
		Iterator<IIRow> iterator;

		Slice slice = null;
		short curShard = Short.MIN_VALUE;
		long curSliceTimestamp = Long.MIN_VALUE;
		int curCol = -1;
		short lastShard = Short.MIN_VALUE;
		long lastSliceTimestamp = Long.MIN_VALUE;
		int lastCol = -1;
		ColumnValueContainer[] containers = null;
        Map<Integer, Dictionary<?>> localDictionaries = Maps.newHashMap();

		Decoder(Iterable<IIRow> kvs) {
			this.iterator = kvs.iterator();
		}

		private void goToNext() {
			if (slice != null) { // was not fetched
				return;
			}

			// NOTE the input keys are ordered
			while (slice == null && iterator.hasNext()) {
                IIRow kv = iterator.next();
				ImmutableBytesWritable k = kv.getKey();
				ImmutableBytesWritable v = kv.getValue();
				decodeKey(k);
//                localDictionaries.put(curCol, kv.getDictionary());

				if (curShard != lastShard
						|| curSliceTimestamp != lastSliceTimestamp) {
					makeNext();
				}
				consumeCurrent(v);
			}
			if (slice == null) {
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

		}

		private void consumeCurrent(ImmutableBytesWritable v) {
			lastShard = curShard;
			lastSliceTimestamp = curSliceTimestamp;
			lastCol = curCol;
		}

		private void makeNext() {
			if (containers != null) {
				slice = new Slice(info, lastShard, lastSliceTimestamp,
						containers);
                slice.setLocalDictionaries(Maps.newHashMap(localDictionaries));
			}
			lastSliceTimestamp = Long.MIN_VALUE;
			lastCol = -1;
			containers = null;
            localDictionaries.clear();
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
					return slice != null;
				}

				@Override
				public Slice next() {
					Slice result = slice;
					slice = null;
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
