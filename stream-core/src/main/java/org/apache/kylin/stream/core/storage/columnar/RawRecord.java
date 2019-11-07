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

        for(int i=0;i<another.dimensions.length;i++){
            this.dimensions[i] = new byte[another.dimensions[i].length];
            System.arraycopy(another.dimensions[i], 0, this.dimensions[i], 0, another.dimensions[i].length);
        }

        for(int i=0;i<another.metrics.length;i++){
            this.metrics[i]=new byte[another.metrics[i].length];
            System.arraycopy(another.metrics[i], 0, this.metrics[i], 0, another.metrics[i].length);
        }

    }

    public RawRecord clone() {
        RawRecord rawRecord = new RawRecord(dimensions.length, metrics.length);

        for(int i=0;i<dimensions.length;i++){
            rawRecord.dimensions[i]=new byte[dimensions[i].length];
            System.arraycopy(dimensions[i], 0, rawRecord.dimensions[i], 0, dimensions[i].length);
        }

        for(int i=0;i<metrics.length;i++){
            rawRecord.metrics[i]=new byte[metrics[i].length];
            System.arraycopy(metrics[i], 0, rawRecord.metrics[i], 0, metrics[i].length);
        }

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
