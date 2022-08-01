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

package org.apache.kylin.common;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PropertiesDelegateTest {

    private PropertiesDelegate delegate;

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put("key_in_prop", "v0");
        properties.put("key_override_external", "v1");

        Properties externalProperties = new Properties();
        externalProperties.put("key_override_external", "v11");
        externalProperties.put("key_in_external", "v2");

        TestExternalConfigLoader testExternalConfigLoader = new TestExternalConfigLoader(externalProperties);
        delegate = new PropertiesDelegate(properties, testExternalConfigLoader);
    }

    @Test
    void testReloadProperties() {
        Properties properties = new Properties();
        delegate.reloadProperties(properties);
        Assertions.assertNull(delegate.getProperty("key_in_prop"));
        Assertions.assertEquals("v11", delegate.getProperty("key_override_external"));

        properties.put("key_override_external", "v1_1");
        delegate.reloadProperties(properties);
        Assertions.assertEquals("v1_1", delegate.getProperty("key_override_external"));
    }

    @Test
    void testGetProperty() {
        Assertions.assertEquals("v0", delegate.getProperty("key_in_prop"));
        Assertions.assertEquals("v1", delegate.getProperty("key_override_external"));
        Assertions.assertEquals("v2", delegate.getProperty("key_in_external"));
        Assertions.assertEquals("v2", delegate.getProperty("key_in_external", "default_value"));

        Assertions.assertNull(delegate.getProperty("key_none_exists"));
        Assertions.assertEquals("default_value", delegate.getProperty("key_none_exists", "default_value"));
    }

    @Test
    void testPut() {
        delegate.put("key_in_prop", "update_v0");
        Assertions.assertEquals("update_v0", delegate.getProperty("key_in_prop"));

        delegate.put("key_override_external", "update_v1");
        Assertions.assertEquals("update_v1", delegate.getProperty("key_override_external"));

        delegate.put("key_in_external", "update_v2");
        Assertions.assertEquals("update_v2", delegate.getProperty("key_in_external"));
    }

    @Test
    void testSetProperty() {
        delegate.setProperty("key_in_prop", "update_v0");
        Assertions.assertEquals("update_v0", delegate.getProperty("key_in_prop"));

        delegate.setProperty("key_override_external", "update_v1");
        Assertions.assertEquals("update_v1", delegate.getProperty("key_override_external"));

        delegate.setProperty("key_in_external", "update_v2");
        Assertions.assertEquals("update_v2", delegate.getProperty("key_in_external"));
    }

    @Test
    void testSize() {
        Assertions.assertEquals(3, delegate.size());
    }

    @Test
    void testEntrySet() {
        Set<Map.Entry<Object, Object>> entries = delegate.entrySet();
        Assertions.assertEquals(3, entries.size());
    }

    @Test
    void testKeys() {
        List<String> keys = new ArrayList<>();
        Enumeration<Object> enumer = delegate.keys();
        while (enumer.hasMoreElements()) {
            keys.add((String) enumer.nextElement());
        }

        Assertions.assertEquals(3, keys.size());

        Assertions.assertEquals("key_in_external, key_in_prop, key_override_external",
                keys.stream().sorted().collect(Collectors.joining(", ")));
    }

    @Test
    void testEqualsAndHashCode() {
        Set<Properties> sets = new HashSet<>();
        sets.add(delegate);
        Assertions.assertEquals(1, sets.size());

        sets.add(delegate);
        Assertions.assertEquals(1, sets.size());

        Assertions.assertEquals(delegate, delegate);

        delegate.reloadProperties(new Properties());
        sets.add(delegate);
        Assertions.assertEquals(2, sets.size());

        Properties properties = new Properties();
        sets.add(properties);
        Assertions.assertEquals(3, sets.size());

        sets.add(properties);
        Assertions.assertEquals(3, sets.size());

        properties.put("1", "v1");
        sets.add(properties);
        Assertions.assertEquals(4, sets.size());
    }
}