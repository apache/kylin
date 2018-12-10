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

package org.apache.kylin.stream.core.storage.columnar;

import java.util.Arrays;

public class RawRecord {
    private byte[][] dimensions;
    private byte[][] metrics;

    public RawRecord(int dimNum, int metricNum) {
        this.dimensions = new byte[dimNum][];
        this.metrics = new byte[metricNum][];
    }

    public RawRecord(byte[][] dimensions, byte[][] metrics) {
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    public void copy(RawRecord another) {
        if (another.getDimensions().length != dimensions.length || another.getMetrics().length != metrics.length) {
            throw new IllegalStateException("cannot copy record with different schema");
        }
        for (int i = 0; i < another.dimensions.length; i++) {
            this.dimensions[i] = another.dimensions[i];
        }
        for (int i = 0; i < another.metrics.length; i++) {
            this.metrics[i] = another.metrics[i];
        }
    }

    public RawRecord clone() {
        RawRecord rawRecord = new RawRecord(dimensions.length, metrics.length);
        System.arraycopy(dimensions, 0, rawRecord.dimensions, 0, dimensions.length);
        System.arraycopy(metrics, 0, rawRecord.metrics, 0, metrics.length);
        return rawRecord;
    }

    public byte[][] getDimensions() {
        return dimensions;
    }

    public void setDimension(int idx, byte[] value) {
        this.dimensions[idx] = value;
    }

    public byte[][] getMetrics() {
        return metrics;
    }

    public void setMetric(int idx, byte[] value) {
        this.metrics[idx] = value;
    }

    @Override
    public String toString() {
        return "Record{" + "dimensions=" + Arrays.toString(dimensions) + ", metrics=" + Arrays.toString(metrics)
                + '}';
    }
}
