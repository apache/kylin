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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.invertedindex.index.ColumnValueContainer;
import org.apache.kylin.invertedindex.index.CompressedValueContainer;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author yangli9
 */
public class IIKeyValueCodec implements KeyValueCodec {

    public static final int SHARD_LEN = 2;
    public static final int TIMEPART_LEN = 8;
    public static final int COLNO_LEN = 2;
    protected final TableRecordInfoDigest digest;

    public IIKeyValueCodec(TableRecordInfoDigest digest) {
        this.digest = digest;
    }

    @Override
    public Collection<IIRow> encodeKeyValue(Slice slice) {
        ArrayList<IIRow> result = Lists.newArrayList();
        ColumnValueContainer[] containers = slice.getColumnValueContainers();
        for (int col = 0; col < containers.length; col++) {
            if (containers[col] instanceof CompressedValueContainer) {
                final IIRow row = collectKeyValues(slice, col, (CompressedValueContainer) containers[col]);
                result.add(row);
            } else {
                throw new IllegalArgumentException("Unknown container class " + containers[col].getClass());
            }
        }
        return result;
    }

    private IIRow collectKeyValues(Slice slice, int col, CompressedValueContainer container) {
        ImmutableBytesWritable key = encodeKey(slice.getShard(), slice.getTimestamp(), col);
        ImmutableBytesWritable value = container.toBytes();
        final Dictionary<?> dictionary = slice.getLocalDictionaries()[col];
        if (dictionary == null) {
            return new IIRow(key, value, new ImmutableBytesWritable(BytesUtil.EMPTY_BYTE_ARRAY));
        } else {
            ByteArray bytes = DictionarySerializer.serialize(dictionary);
            return new IIRow(key, value, new ImmutableBytesWritable(bytes.array(), bytes.offset(), bytes.length()));
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

    private static TableRecordInfoDigest createDigest(int nColumns,  boolean[] isMetric, String[] dataTypes, Dictionary<?>[] dictionaries) {
        int[] dictMaxIds = new int[nColumns];
        int[] lengths = new int[nColumns];
        final boolean emptyDictionary = Array.isEmpty(dictionaries);
        for (int i = 0; i < nColumns; ++i) {
            if (isMetric[i]) {
                final FixedLenMeasureCodec<?> fixedLenMeasureCodec = FixedLenMeasureCodec.get(DataType.getInstance(dataTypes[i]));
                lengths[i] = fixedLenMeasureCodec.getLength();
            } else {
                if (emptyDictionary) {
                    final DataType dataType = DataType.getInstance(dataTypes[i]);
                    if (dataType.isNumberFamily()) {
                        lengths[i] = 16;
                    } else if (dataType.isStringFamily()) {
                        lengths[i] = 256;
                    } else if (dataType.isDateTimeFamily()) {
                        lengths[i] = 19;
                    } else {
                        throw new RuntimeException("invalid data type:" + dataType);
                    }
                    dictMaxIds[i] = Integer.MAX_VALUE;
                } else {
                    final Dictionary<?> dictionary = dictionaries[i];
                    lengths[i] = dictionary.getSizeOfId();
                    dictMaxIds[i] = dictionary.getMaxId();
                }
            }
        }
        // offsets
        int pos = 0;
        int[] offsets = new int[nColumns];
        for (int i = 0; i < nColumns; i++) {
            offsets[i] = pos;
            pos += lengths[i];
        }

        int byteFormLen = pos;

        return new TableRecordInfoDigest(nColumns, byteFormLen, offsets, dictMaxIds, lengths, isMetric, dataTypes);
    }

    protected static class IIRowDecoder implements Iterable<Slice> {

        protected final TableRecordInfoDigest incompleteDigest;
        protected final Iterator<IIRow> iiRowIterator;
        protected Iterator<IIRow> feedingIterator;//this is for extending

        protected IIRowDecoder(TableRecordInfoDigest digest, Iterator<IIRow> iiRowIterator) {
            this.incompleteDigest = digest;
            this.iiRowIterator = iiRowIterator;
            this.feedingIterator = this.iiRowIterator;
        }

        @Override
        public Iterator<Slice> iterator() {
            return new Iterator<Slice>() {
                @Override
                public boolean hasNext() {
                    return iiRowIterator.hasNext();
                }

                @Override
                public Slice next() {
                    int columns = 0;
                    ColumnValueContainer[] valueContainers = new ColumnValueContainer[incompleteDigest.getColumnCount()];
                    Dictionary<?>[] localDictionaries = new Dictionary<?>[incompleteDigest.getColumnCount()];
                    boolean firstTime = true;
                    short curShard = 0;
                    long curTimestamp = 0;
                    short lastShard = 0;
                    long lastTimestamp = 0;

                    while (feedingIterator.hasNext() && columns < incompleteDigest.getColumnCount()) {
                        final IIRow row = feedingIterator.next();
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
                        if (incompleteDigest.isMetrics(curCol)) {
                            CompressedValueContainer c = new CompressedValueContainer(incompleteDigest, curCol, 0);
                            c.fromBytes(row.getValue());
                            valueContainers[curCol] = c;
                        } else {
                            final ImmutableBytesWritable dictBytes = row.getDictionary();
                            if (dictBytes.getLength() != 0) {
                                final Dictionary<?> dictionary = DictionarySerializer.deserialize(new ByteArray(dictBytes.get(), dictBytes.getOffset(), dictBytes.getLength()));
                                CompressedValueContainer c = new CompressedValueContainer(dictionary.getSizeOfId(), dictionary.getMaxId() - dictionary.getMinId() + 1, 0);
                                c.fromBytes(row.getValue());
                                valueContainers[curCol] = c;
                                localDictionaries[curCol] = dictionary;
                            } else {
                                CompressedValueContainer c = new CompressedValueContainer(incompleteDigest.length(curCol), incompleteDigest.getMaxID(curCol) - 0 + 1, 0);
                                c.fromBytes(row.getValue());
                                valueContainers[curCol] = c;
                            }
                        }
                        columns++;
                        lastShard = curShard;
                        lastTimestamp = curTimestamp;
                        firstTime = false;
                    }
                    Preconditions.checkArgument(columns == incompleteDigest.getColumnCount(), "column count is " + columns + " should be equals to incompleteDigest.getColumnCount() " + incompleteDigest.getColumnCount());

                    TableRecordInfoDigest digest = createDigest(columns, incompleteDigest.getIsMetric(), incompleteDigest.getMetricDataTypes(), localDictionaries);
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

}
