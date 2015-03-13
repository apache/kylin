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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.index.*;

import java.io.*;
import java.util.*;

/**
 * @author yangli9
 */
public class IIKeyValueCodec implements KeyValueCodec {

	public static final int SHARD_LEN = 2;
	public static final int TIMEPART_LEN = 8;
	public static final int COLNO_LEN = 2;
    private final TableRecordInfoDigest digest;

    public IIKeyValueCodec(TableRecordInfoDigest digest) {
        this.digest = digest;
	}

    @Override
	public Collection<IIRow> encodeKeyValue(Slice slice) {
		ArrayList<IIRow> result = Lists
				.newArrayList();
		ColumnValueContainer[] containers = slice.getColumnValueContainers();
		for (int col = 0; col < containers.length; col++) {
			if (containers[col] instanceof CompressedValueContainer) {
                final IIRow row = collectKeyValues(slice, col, (CompressedValueContainer) containers[col]);
                result.add(row);
            } else {
                throw new IllegalArgumentException("Unknown container class "
						+ containers[col].getClass());
            }
        }
		return result;
	}

	private IIRow collectKeyValues(Slice slice, int col, CompressedValueContainer container) {
		ImmutableBytesWritable key = encodeKey(slice.getShard(), slice.getTimestamp(), col);
		ImmutableBytesWritable value = container.toBytes();
        final Dictionary<?> dictionary = slice.getLocalDictionaries().get(col);
        return new IIRow(key, value, serialize(dictionary));
	}

    private static Dictionary<?> deserialize(ImmutableBytesWritable dictBytes) {
        try {
            final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(dictBytes.get(), dictBytes.getOffset(), dictBytes.getLength()));
            final String type = dataInputStream.readUTF();
            final Dictionary dictionary = ClassUtil.forName(type, Dictionary.class).newInstance();
            dictionary.readFields(dataInputStream);
            return dictionary;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ImmutableBytesWritable serialize(Dictionary<?> dict) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            return new ImmutableBytesWritable(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        return new IIRowDecoder(digest, kvs.iterator());
//		return new Decoder(kvs, incompleteDigest);
	}

    private static class IIRowDecoder implements Iterable<Slice> {

        private final TableRecordInfoDigest incompleteDigest;
        private final Iterator<IIRow> iterator;

        private IIRowDecoder(TableRecordInfoDigest digest, Iterator<IIRow> iterator) {
            this.incompleteDigest = digest;
            this.iterator = iterator;
        }

        @Override
        public Iterator<Slice> iterator() {
            return new Iterator<Slice>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Slice next() {
                    int columns = 0;
                    ColumnValueContainer[] valueContainers = new ColumnValueContainer[incompleteDigest.getColumnCount()];
                    Map<Integer, Dictionary<?>> localDictionaries = Maps.newHashMap();
                    boolean firstTime = true;
                    short curShard = 0;
                    long curTimestamp = 0;
                    short lastShard = 0;
                    long lastTimestamp = 0;

                    while (iterator().hasNext() && columns < incompleteDigest.getColumnCount()) {
                        final IIRow row = iterator.next();
                        final ImmutableBytesWritable key = row.getKey();
                        int i = key.getOffset();
                        curShard = (short) BytesUtil.readUnsigned(key.get(), i, SHARD_LEN);
                        i += SHARD_LEN;
                        curTimestamp = BytesUtil.readLong(key.get(), i, TIMEPART_LEN);
                        i += TIMEPART_LEN;

                        if (!firstTime) {
                            Preconditions.checkArgument(curShard == lastShard, "shard should be equals in one slice, curShard is" + curShard + " lastShard is " + lastShard);
                            Preconditions.checkArgument(curTimestamp == lastTimestamp, "timestamp should be equals in one slice, curTimestamp is" + curTimestamp + " lastTimestamp is " + lastTimestamp);
                        }

                        int curCol = BytesUtil.readUnsigned(key.get(), i, COLNO_LEN);
                        final Dictionary<?> dictionary = deserialize(row.getDictionary());
                        if (incompleteDigest.isMetrics(curCol)) {
                            CompressedValueContainer c = new CompressedValueContainer(incompleteDigest, curCol, 0);
                            c.fromBytes(row.getValue());
                            valueContainers[curCol] = c;
                        } else {
                            CompressedValueContainer c = new CompressedValueContainer(dictionary.getSizeOfId(), dictionary.getMaxId() - dictionary.getMinId() + 1, 0);
                            c.fromBytes(row.getValue());
                            valueContainers[curCol] = c;
                        }
                        localDictionaries.put(curCol, dictionary);
                        columns++;
                        lastShard = curShard;
                        lastTimestamp = curTimestamp;
                        firstTime = false;
                    }
                    Preconditions.checkArgument(columns == incompleteDigest.getColumnCount(), "column count is " + columns + " should be equals to incompleteDigest.getColumnCount() " + incompleteDigest.getColumnCount());

                    TableRecordInfoDigest digest = TableRecordInfo.createDigest(columns, incompleteDigest.getIsMetric(), incompleteDigest.getMetricDataTypes(), localDictionaries);
                    Slice slice = new Slice(digest, curShard, curTimestamp, valueContainers);
                    slice.setLocalDictionaries(localDictionaries);
                    return slice;
                }


                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

    }


	private static class Decoder implements Iterable<Slice> {

        private final TableRecordInfoDigest digest;
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

		Decoder(Iterable<IIRow> kvs, TableRecordInfoDigest digest) {
            this.digest = digest;
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
                final Dictionary<?> dictionary = deserialize(kv.getDictionary());
                final CompressedValueContainer c = new CompressedValueContainer(digest, curCol, 0);
                c.fromBytes(kv.getValue());
                addContainer(curCol, c);
                localDictionaries.put(curCol, dictionary);
                if (localDictionaries.size() < digest.getColumnCount()) {
                    continue;
                }

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
				slice = new Slice(digest, lastShard, lastSliceTimestamp,
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
				containers = new ColumnValueContainer[digest.getColumnCount()];
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
