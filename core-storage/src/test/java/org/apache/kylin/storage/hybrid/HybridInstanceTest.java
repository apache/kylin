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

package org.apache.kylin.storage.hybrid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.junit.Test;

/**
 * Unit tests for class {@link HybridInstance}.
 *
 * @see HybridInstance
 */
public class HybridInstanceTest {


    @Test
    public void testGetCost() {
        HybridInstance hybridInstance = new HybridInstance();

        assertEquals(Integer.MAX_VALUE, hybridInstance.getCost());
    }

    @Test
    public void testIsCapable() {
        HybridInstance hybridInstance = new HybridInstance();
        LinkedHashSet<TblColRef> linkedHashSet = new LinkedHashSet<>();
        TupleInfo tupleInfo = new TupleInfo();
        Tuple tuple = new Tuple(tupleInfo);
        SQLDigest sQLDigest = new SQLDigest("6Bw^[(:[hOd",
                linkedHashSet,
                new LinkedList<>(),
                tuple.getAllColumns(),
                linkedHashSet,
                new ConcurrentHashMap<>(),
                false,
                linkedHashSet,
                new LinkedList<>(),
                new LinkedList<>(),
                new LinkedList<>(),
                linkedHashSet,
                linkedHashSet,
                linkedHashSet,
                null,
                null,
                tuple.getAllColumns(),
                new LinkedList<>(),
                true,
                true,
                false,
                new LinkedHashSet<>());
        CapabilityResult capabilityResult = hybridInstance.isCapable(sQLDigest);

        assertNotNull(capabilityResult);
        assertFalse(capabilityResult.capable);
        assertEquals(2147483646, capabilityResult.cost);
    }


}
