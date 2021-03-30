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

package org.apache.kylin.metrics.lib.impl;

import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;

/**
 * A Reservoir will drop message.
 */
public class StubReservoir implements ActiveReservoir {

    public void addListener(ActiveReservoirListener listener) {
    }

    public void removeListener(ActiveReservoirListener listener) {
    }

    public void removeAllListener() {
    }

    public void setHAListener(ActiveReservoirListener listener) {
    }

    public void update(Record record) {
    }

    public int size() {
        return 0;
    }

    public void start() {
    }

    public void stop() {
    }

    public void close() {
    }
}
