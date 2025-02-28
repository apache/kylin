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
package org.apache.kylin.measure.topn;

import java.nio.ByteBuffer;

/**
 * Given an array of double in ascending order, serialize them into very compact bytes.
 *
 * http://bitcharmer.blogspot.co.uk/2013/12/how-to-serialise-array-of-doubles-with.html
 */
public class DoubleDeltaSerializer implements java.io.Serializable {

    // first 32 bits stores meta info
    static final int PRECISION_BITS = 3;
    static final int DELTA_SIZE_BITS = 6;
    static final int LENGTH_BITS = 23;

    static final long[] MASKS = new long[64];
    {
        for (int i = 0; i < MASKS.length; i++) {
            MASKS[i] = (1L << i) - 1;
        }
    }

    private final int precision;
    private final int multiplier;

    transient ThreadLocal<long[]> deltasThreadLocal;

    public DoubleDeltaSerializer() {
        this(2);
    }

    /**
     * @param precision round to specified number of digits after decimal point
     */
    public DoubleDeltaSerializer(int precision) {
        checkFitInBits(precision, PRECISION_BITS);

        this.precision = precision;
        this.multiplier = (int) Math.pow(10, precision);
    }

    public void serialize(double[] values, ByteBuffer buf) {
        long[] deltas = calculateDeltas(values);
        int deltaSize = maxDeltaSize(deltas, values.length - 1);

        checkFitInBits(deltaSize, DELTA_SIZE_BITS);
        checkFitInBits(values.length, LENGTH_BITS);

        int meta = (int) precision << (LENGTH_BITS + DELTA_SIZE_BITS) | (int) deltaSize << LENGTH_BITS | values.length;
        buf.putInt(meta);

        if (values.length == 0)
            return;

        buf.putLong(roundAndPromote(values[0]));
        putDeltas(deltas, values.length - 1, deltaSize, buf);
    }

    private void putDeltas(long[] deltas, int len, int deltaSize, ByteBuffer buf) {
        long bits = 0;
        int free = Long.SIZE;
        for (int i = 0; i < len; i++) {
            if (free >= deltaSize) {
                bits |= deltas[i] << (free - deltaSize);
                free -= deltaSize;
                if (free == 0) {
                    buf.putLong(bits);
                    bits = 0;
                    free = Long.SIZE;
                }
            } else {
                bits |= deltas[i] >> (deltaSize - free);
                buf.putLong(bits);
                free = (Long.SIZE - (deltaSize - free));
                bits = deltas[i] << free;
            }
        }

        if (free < Long.SIZE) {
            buf.putLong(bits);
        }
    }

    private int maxDeltaSize(long[] deltas, int len) {
        long maxDelta = 0;
        for (int i = 0; i < len; i++) {
            maxDelta = Math.max(maxDelta, deltas[i]);
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxDelta);
    }

    private long[] calculateDeltas(double[] values) {
        int len = values.length - 1;
        len = Math.max(0, len);

        if (deltasThreadLocal == null) {
            deltasThreadLocal = new ThreadLocal<>();
        }

        long[] deltas = deltasThreadLocal.get();
        if (deltas == null || deltas.length < len) {
            deltas = new long[len];
            deltasThreadLocal.set(deltas);
        }

        if (len == 0)
            return deltas;

        long current = roundAndPromote(values[0]);
        for (int i = 0; i < len; i++) {
            long next = roundAndPromote(values[i + 1]);
            deltas[i] = next - current;
            assert deltas[i] >= 0;
            current = next;
        }
        return deltas;
    }

    private long roundAndPromote(double value) {
        return (long) (value * multiplier + .5d);
    }

    private void checkFitInBits(int num, int bits) {
        if (num >= (1 << bits))
            throw new IllegalArgumentException();
    }

    public double[] deserialize(ByteBuffer buf) {
        int meta = buf.getInt();
        int len = meta & ((1 << LENGTH_BITS) - 1);

        double[] result = new double[len];
        deserialize(buf, meta, result);

        return result;
    }

    public int deserialize(ByteBuffer buf, double[] result) {
        return deserialize(buf, buf.getInt(), result);
    }

    private int deserialize(ByteBuffer buf, int meta, double[] result) {
        int precision = meta >>> (DELTA_SIZE_BITS + LENGTH_BITS);
        assert precision == this.precision;
        int deltaSize = (meta >>> LENGTH_BITS) & ((1 << DELTA_SIZE_BITS) - 1);
        int len = meta & ((1 << LENGTH_BITS) - 1);

        if (len == 0)
            return 0;
        assert result.length >= len;

        long current = buf.getLong();
        result[0] = (double) current / multiplier;

        long bits = 0;
        int left = 0;
        for (int i = 1; i < len; i++) {
            long delta = 0;

            if (left >= deltaSize) {
                delta = (bits >> (left - deltaSize)) & MASKS[deltaSize];
                left -= deltaSize;
            } else {
                int more = deltaSize - left;
                delta = (bits & MASKS[left]) << more;
                bits = buf.getLong();
                left = Long.SIZE;
                delta |= (bits >> (left - more)) & MASKS[more];
                left -= more;
            }

            current += delta;
            result[i] = (double) current / multiplier;
        }
        return len;
    }
}
