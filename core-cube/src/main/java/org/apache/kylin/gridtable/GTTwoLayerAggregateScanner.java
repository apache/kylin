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

package org.apache.kylin.gridtable;

import java.io.IOException;
import java.util.Iterator;

/**
 * GTTwoLayerAggregateScanner will do two-layer aggregations
 * 1. By first layer, it will use GTAggregateTransformScanner to get existing defined aggregations based on pre-calculated data
 * 2. By second layer, it will do the aggregation by another different function, like stddev
 */
public class GTTwoLayerAggregateScanner implements IGTScanner {

    protected final IGTScanner inputScanner;
    private final GTAggregateTransformScanner secondLayerInputScanner;
    private final GTAggregateScanner outputScanner;

    public GTTwoLayerAggregateScanner(IGTScanner inputScanner, GTScanRequest req, boolean spillEnabled) {
        this.inputScanner = inputScanner;
        this.secondLayerInputScanner = new GTAggregateTransformScanner(inputScanner, req);
        this.outputScanner = new GTAggregateScanner(secondLayerInputScanner, req, spillEnabled);
    }

    @Override
    public GTInfo getInfo() {
        return inputScanner.getInfo();
    }

    @Override
    public void close() throws IOException {
        inputScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return outputScanner.iterator();
    }

    public long getFirstLayerInputRowCount() {
        return secondLayerInputScanner.getInputRowCount();
    }

    public long getSecondLayerInputRowCount() {
        return outputScanner.getInputRowCount();
    }
}