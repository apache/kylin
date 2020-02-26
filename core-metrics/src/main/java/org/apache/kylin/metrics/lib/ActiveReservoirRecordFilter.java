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

/**
 * A filter used to determine whether or not an active reservoir should be reported, among other things.
 */
public interface ActiveReservoirRecordFilter {

    /**
     * Matches all active reservoirs, regardless of type or name.
     */
    ActiveReservoirRecordFilter ALL = new ActiveReservoirRecordFilter() {
        @Override
        public boolean matches(String name, ActiveReservoir activeReservoir) {
            return true;
        }
    };

    /**
     * Returns {@code true} if the active reservoir matches the filter; {@code false} otherwise.
     *
     * @param name            the active reservoir's name
     * @param activeReservoir the active reservoir
     * @return {@code true} if the active reservoir matches the filter
     */
    default boolean matches(String name, ActiveReservoir activeReservoir) {
        return true;
    }

    /**
     * This method is used to check and whether to filter a Record on listener side
     */
    default boolean checkRecord(Record record) {
        return true;
    }
}
