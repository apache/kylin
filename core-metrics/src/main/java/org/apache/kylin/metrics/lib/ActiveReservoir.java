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

package org.apache.kylin.metrics.lib;

import java.io.Closeable;

/**
 * Reservoir for metrics event, a Reservoir(something like cache/buffer)'s duty is store metrics event temporarily
 * and emit events to external Sink by notifying specific ActiveReservoirListener.
 */
public interface ActiveReservoir extends Closeable {

    /**
     * @return how many metrics message was currently cached(not emit)
     */
    int size();

    /**
     * stage metrics message into Reservoir, but whether to emit it to external storage
     * immediately is decided by specific implementation
     */
    void update(Record record);

    /**
     * add listener which is in charge of send metrics event update
     */
    void addListener(ActiveReservoirListener listener);

    void removeListener(ActiveReservoirListener listener);

    void removeAllListener();

    /**
     * A backup listener, it will be called when one of the previous listener failed.
     */
    void setHAListener(ActiveReservoirListener listener);

    /**
     * do some prepare to accept metrics event
     */
    void start();

    void stop();
}
