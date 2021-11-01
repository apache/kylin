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

package org.apache.kylin.rest.response;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class SQLResponseTest {

    @Test
    public void testInterfaceConsistency() throws IOException {
        String[] attrArray = new String[] { "columnMetas", "results", "cube", "cuboidIds",
                "realizationTypes", "affectedRowCount", "isException",
                "exceptionMessage", "duration", "partial", "totalScanCount", "hitExceptionCache",
                "storageCacheUsed", "sparkPool", "pushDown", "traceUrl", "totalScanBytes",
                "totalScanFiles", "metadataTime", "totalSparkScanTime", "traces"};

        SQLResponse sqlResponse = new SQLResponse(null, null, "learn_cube", 100, false, null, false, false);
        String jsonStr = JsonUtil.writeValueAsString(sqlResponse);
        System.out.println(jsonStr);

        JsonNode jnode = JsonUtil.readValueAsTree(jsonStr);
        assertEquals(jnode.size(), attrArray.length);
        for (String attr : attrArray) {
            Assert.assertTrue(attr + " doesn't exist", jnode.has(attr));
        }
    }
}
