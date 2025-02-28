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

package org.apache.kylin.engine.spark.planner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.hash.HashFunction;
import org.apache.kylin.guava30.shaded.common.hash.Hasher;
import org.apache.kylin.guava30.shaded.common.hash.Hashing;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.measure.hllc.RegisterType;
import org.apache.spark.sql.Row;

public class HLLCountCounter {

    public static final int HLL = 1;
    public static final int PLAIN = 0;
    public static final int DEFAULT_BUFFER_SIZE = 1024 * 128;
    public static final int DEFAULT_PRECISION = 14;

    private final HashFunction hf;
    private final List<TupleCountCounter> counterList;

    public HLLCountCounter(int precision, Map<BitSet, List<Integer>> cuboidIndexMap) {
        this.hf = Hashing.murmur3_128();
        this.counterList = cuboidIndexMap.entrySet().stream().map(
                e -> new TupleCountCounter(e.getKey(), e.getValue(), new HLLCounter(precision, RegisterType.DENSE)))
                .collect(ImmutableList.toImmutableList());
    }

    public void readRow(final Row row) {
        if (row == null) {
            return;
        }
        // cache column value hash
        final Map<Integer, Long> columnCache = Maps.newHashMap();
        for (TupleCountCounter counter : counterList) {
            counter.accumulate(counter.indexList().stream()
                    // add column ordinal to the hash value to distinguish between (a,b) and (b,a)
                    .map(index -> columnCache.computeIfAbsent(index, i -> (hashLong(row.get(i)) + i)))
                    .reduce(0L, Long::sum));
        }
    }

    public Iterator<TupleCountCounter> counterIterator() {
        return counterList.iterator();
    }

    private long hashLong(Object value) {
        Hasher hasher = hf.newHasher();
        if (value == null) {
            hasher.putInt(0);
        } else {
            hasher.putString(value.toString(), StandardCharsets.UTF_8);
        }
        return hasher.hash().asLong();
    }

    public static long readCount(byte[] bytes, int precision) throws IOException {
        int flag = readFlag(bytes);
        switch (flag) {
        case PLAIN:
            return readPlainCount(bytes);
        case HLL:
            return readHllCounter(bytes, precision).getCountEstimate();
        default:
            throw new IllegalArgumentException("Unknown bytes flag " + flag);
        }
    }

    public static int readFlag(byte[] bytes) {
        return ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).getInt();
    }

    public static long readPlainCount(byte[] bytes) {
        return ByteBuffer.wrap(bytes, 0, bytes.length - Integer.BYTES).getLong();
    }

    public static HLLCounter readHllCounter(byte[] bytes, int precision) throws IOException {
        HLLCounter hll = new HLLCounter(precision);
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, bytes.length - Integer.BYTES);
        hll.readRegisters(buffer);
        return hll;
    }

    public static byte[] plainBytes(final long count) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        buffer.putLong(count);
        buffer.putInt(PLAIN);
        return copyBytes(buffer);
    }

    public static byte[] hllBytes(final HLLCounter hll) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        hll.writeRegisters(buffer);
        buffer.putInt(HLL);
        return copyBytes(buffer);
    }

    private static byte[] copyBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, bytes, 0, buffer.position());
        return bytes;
    }

}
