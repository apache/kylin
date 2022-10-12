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

import static org.apache.kylin.metrics.lib.impl.MetricsSystem.Metrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirRecordFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricsSystemTest {

    @Test
    void testDuplicateRegister() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            String name = "test1";
            Metrics.register(name, new StubReservoir());
            Metrics.register(name, new StubReservoir());
        });
    }

    @Test
    void testNullRegister1() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            Metrics.register(null, new StubReservoir());
        });
    }

    @Test
    void testActiveReservoir() {
        //Remove all ActiveReservoirs
        Metrics.removeActiveReservoirMatching(ActiveReservoirRecordFilter.ALL);
        assertEquals(0, Metrics.getActiveReservoirs().size());

        //Get all the ActiveReservoirs
        int n = 10;
        for (int i = 0; i < n; i++) {
            Metrics.register("ActiveReservoir-" + i, new StubReservoir());
        }
        assertEquals(n, Metrics.getActiveReservoirs().size());

        String name = "test2";
        ActiveReservoir activeReservoir = new StubReservoir();
        Metrics.register(name, activeReservoir);

        //Get ActiveReservoir by name
        assertEquals(activeReservoir, Metrics.activeReservoir(name));
        //Remove ActiveReservoir by name
        assertTrue(Metrics.removeActiveReservoir(name));
        assertFalse(Metrics.removeActiveReservoir(name));
    }
}
