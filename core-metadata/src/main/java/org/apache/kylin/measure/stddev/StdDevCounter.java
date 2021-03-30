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

package org.apache.kylin.measure.stddev;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class StdDevCounter implements Serializable {

    protected double variance;
    protected double sum;
    protected long count;

    public StdDevCounter(double variance, double sum, long count) {
        this.variance = variance;
        this.sum = sum;
        this.count = count;
    }

    public StdDevCounter() {
        this(0, 0, 0);
    }

    public StdDevCounter(double elem) {
        this(0, elem, 1L);
    }

    public StdDevCounter(StdDevCounter another) {
        this(another.variance, another.sum, another.count);
    }

    public void add(double elem) {
        merge(new StdDevCounter(elem));
    }

    public void merge(StdDevCounter another) {
        if (another == null || another.count == 0) {
            return;
        } else if (count == 0) {
            variance = another.variance;
            sum = another.sum;
            count = another.count;
            return;
        }
        double t = ((another.count) / (double) count) * sum - another.sum;
        this.variance += another.variance
                + ((count / (double) another.count) / ((double) count + another.count)) * t * t;
        this.sum += another.sum;
        this.count += another.count;
    }

    public double getStandardDeviation() {
        if (count == 0 || count == 1)
            return 0;
        return Math.sqrt(variance / count);
    }

    public void writeRegisters(ByteBuffer out) {
        out.putDouble(variance);
        out.putDouble(sum);
        out.putLong(count);
    }

    public void readRegisters(ByteBuffer in) {
        variance = in.getDouble();
        sum = in.getDouble();
        count = in.getLong();
    }

    public int sizeOfBytes() {
        return (Double.SIZE * 2 + Long.SIZE) / Byte.SIZE;
    }
}