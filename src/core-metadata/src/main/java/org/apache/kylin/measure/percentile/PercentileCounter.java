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

package org.apache.kylin.measure.percentile;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class PercentileCounter implements Serializable {
    public static final int DEFAULT_PERCENTILE_ACCURACY = 100;
    private static final double INVALID_QUANTILE_RATIO = -1;

    double compression;
    double quantileRatio;

    transient TDigest registers;

    public PercentileCounter(double compression) {
        this(compression, INVALID_QUANTILE_RATIO);
    }

    public PercentileCounter(PercentileCounter another) {
        this(another.compression, another.quantileRatio);
        merge(another);
    }

    public PercentileCounter(double compression, double quantileRatio) {
        this.compression = compression;
        this.quantileRatio = quantileRatio;
        reInitRegisters();
    }

    private void reInitRegisters() {
        this.registers = TDigest.createAvlTreeDigest(this.compression);
    }

    public void add(double v) {
        registers.add(v);
    }

    public void merge(PercentileCounter counter) {
        assert this.compression == counter.compression;
        registers.add(counter.registers);
    }

    public Double getResultEstimate() {
        if (registers.size() == 0) {
            return null;
        }
        return registers.quantile(quantileRatio);
    }

    public Double getResultEstimateWithQuantileRatio(double quantileRatio) {
        if (registers.size() == 0) {
            return null;
        }
        return registers.quantile(quantileRatio);
    }

    public void writeRegisters(ByteBuffer out) {
        registers.compress();
        registers.asSmallBytes(out);
    }

    public void readRegisters(ByteBuffer in) {
        registers = AVLTreeDigest.fromBytes(in);
        compression = registers.compression();
    }

    public int getBytesEstimate() {
        return maxLength();
    }

    /**
     * the Percentile is non-fixed length that is affected by the count and 
     * compression mainly. so we collect some statistics
     * about it and do some analysis, which get from test and the T-digest Paper.
     *</p>
     * As a result, we conclude its regular pattern by Stata , a tool help construct function model.
     * 0 - 2 * compression it grows a linear function which is easily derived from T-digest Algorithm
     * 2 * compression - 50000000 it grows roughly a log and the accuracy 
     * increases with the number of samples observed
     *
     * @param count
     * @return
     */
    public double getBytesEstimate(double count) {
        if (count <= 2 * compression)
            return 16 + count * 5;

        switch ((int) compression) {
        case 100:
            return 597.9494 * Math.log1p(count) - 2358.987;
        case 1000:
            return 5784.34 * Math.log1p(count) - 35030.97;
        case 10000:
            return 54313.96 * Math.log1p(count) - 438988.8;
        default:
            return 0.0;
        }
    }

    public int maxLength() {
        switch ((int) compression) {
        case 100:
            return 16 * 1024;
        case 1000:
            return 128 * 1024;
        case 10000:
            return 1024 * 1024;
        default:
            return 16 * 1024;
        }
    }

    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        AVLTreeDigest.fromBytes(in);
        int total = in.position() - mark;
        in.position(mark);
        return total;
    }

    public void clear() {
        reInitRegisters();
    }

    public double getCompression() {
        return compression;
    }

    public double getQuantileRatio() {
        return quantileRatio;
    }

    public TDigest getRegisters() {
        return registers;
    }
}
