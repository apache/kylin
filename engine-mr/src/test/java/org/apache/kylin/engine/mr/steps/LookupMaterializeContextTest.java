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

package org.apache.kylin.engine.mr.steps;

import org.apache.kylin.engine.mr.LookupMaterializeContext;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LookupMaterializeContextTest {
    @Test
    public void parseAndToStringTest() throws Exception {
        LookupMaterializeContext context = new LookupMaterializeContext(null);
        context.addLookupSnapshotPath("lookup1", "/ext_snapshot/uuid1");
        context.addLookupSnapshotPath("lookup2", "/ext_snapshot/uuid2");

        String lookupSnapshotsStr = context.getAllLookupSnapshotsInString();
        Map<String, String> lookupSnapshotMap = LookupMaterializeContext.parseLookupSnapshots(lookupSnapshotsStr);
        assertEquals(2, lookupSnapshotMap.size());
        assertEquals("/ext_snapshot/uuid1", lookupSnapshotMap.get("lookup1"));
        assertEquals("/ext_snapshot/uuid2", lookupSnapshotMap.get("lookup2"));
    }
}
