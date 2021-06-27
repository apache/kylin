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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.Test;

import com.google.common.collect.Maps;

public class RecordEventTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSetEventType() {
        new RecordEvent(null, System.currentTimeMillis());
    }

    @Test
    public void testBasic() throws IOException {
        String type = "TEST";
        String localHostname;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            localHostname = addr.getHostName() + ":" + addr.getHostAddress();
        } catch (UnknownHostException e) {
            localHostname = "Unknown";
        }
        long time = System.currentTimeMillis();
        RecordEvent event = new RecordEvent(type, localHostname, time);

        assertEquals(type, event.getEventType());
        assertEquals(localHostname, event.getHost());
        assertTrue(time == event.getTime());

        String key = "PROJECT";
        String value = "test";
        event.put(key, value);
        assertEquals(value, event.remove(key));

        int len1 = event.size();
        Map<String, Object> entryMap = Maps.newHashMap();
        for (int i = 0; i < 5; i++) {
            entryMap.put(key + "-" + i, value + "-" + i);
        }
        event.putAll(entryMap);
        assertEquals(entryMap.size(), event.size() - len1);

        assertTrue(event.clone().equals(event));

        Map<String, Object> rawValue = JsonUtil.readValue(event.getValue(), Map.class);
        assertEquals(event.size() - 1, rawValue.size());

        assertNull(rawValue.get(RecordEvent.RecordReserveKeyEnum.EVENT_SUBJECT.toString()));

        event.clear();
        assertTrue(event.isEmpty());
    }
}
