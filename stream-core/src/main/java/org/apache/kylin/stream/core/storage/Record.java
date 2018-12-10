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

package org.apache.kylin.stream.core.storage;

import java.util.Arrays;

public class Record {
    private String[] dimensions;
    private Object[] metrics;

    public Record(int dimNum, int metricNum) {
        this.dimensions = new String[dimNum];
        this.metrics = new Object[metricNum];
    }

    public Record copy() {
        Record record = new Record(dimensions.length, metrics.length);
        System.arraycopy(dimensions, 0, record.dimensions, 0, dimensions.length);
        System.arraycopy(metrics, 0, record.metrics, 0, metrics.length);
        return record;
    }

    public void setDimensions(String[] dimensions) {
        this.dimensions = dimensions;
    }

    public void setMetrics(Object[] metrics) {
        this.metrics = metrics;
    }

    public String[] getDimensions() {
        return dimensions;
    }

    public void setDimension(int idx, String value) {
        this.dimensions[idx] = value;
    }

    public Object[] getMetrics() {
        return metrics;
    }

    public void setMetric(int idx, Object value) {
        this.metrics[idx] = value;
    }

    @Override
    public String toString() {
        return "Record{" + "dimensions=" + Arrays.toString(dimensions) + ", metrics=" + Arrays.toString(metrics)
                + '}';
    }
}
