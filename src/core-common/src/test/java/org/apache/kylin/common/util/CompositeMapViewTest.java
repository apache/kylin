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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

class CompositeMapViewTest {

    private CompositeMapView<String, String> compositeMapView;

    @BeforeEach
    void setup() {
        compositeMapView = prepareData();
    }

    private CompositeMapView<String, String> prepareData() {
        val map1 = Maps.<String, String> newHashMap();
        map1.put("k1", "v1");
        map1.put("k2", "v2");
        map1.put("k3", "v3");

        val map2 = Maps.<String, String> newHashMap();
        map2.put("k1", "vv1");
        map2.put("kk2", "vv2");
        map2.put("kk3", "v3");
        return new CompositeMapView<>(map1, map2);
    }

    @Test
    void testSize() {
        Assertions.assertEquals(5, compositeMapView.size());
    }

    @Test
    void testIsEmpty() {
        {
            Assertions.assertFalse(compositeMapView.isEmpty());
        }

        {
            CompositeMapView<String, String> compositeMapView = new CompositeMapView<>(Maps.newHashMap(),
                    Maps.newHashMap());
            Assertions.assertTrue(compositeMapView.isEmpty());
        }
    }

    @Test
    void testIsEmpty_Null() {
        val emptyMap = Collections.emptyMap();
        Assertions.assertThrows(NullPointerException.class, () -> new CompositeMapView<>(null, null));

        Assertions.assertThrows(NullPointerException.class, () -> new CompositeMapView<>(emptyMap, null));

        Assertions.assertThrows(NullPointerException.class, () -> new CompositeMapView<>(null, emptyMap));
    }

    @Test
    void testContainsKey() {

        Assertions.assertTrue(compositeMapView.containsKey("k1"));
        Assertions.assertTrue(compositeMapView.containsKey("kk3"));
        Assertions.assertTrue(compositeMapView.containsKey("k2"));

        Assertions.assertFalse(compositeMapView.containsKey("noKey"));
    }

    @Test
    void testContainsValue() {

        Assertions.assertTrue(compositeMapView.containsValue("v1"));
        Assertions.assertTrue(compositeMapView.containsValue("vv2"));
        Assertions.assertTrue(compositeMapView.containsValue("v2"));

        Assertions.assertFalse(compositeMapView.containsValue("noValue"));
    }

    @Test
    void testGetKey() {

        //both
        Assertions.assertEquals("vv1", compositeMapView.get("k1"));
        //left only
        Assertions.assertEquals("v2", compositeMapView.get("k2"));
        //right only
        Assertions.assertEquals("v3", compositeMapView.get("kk3"));

        Assertions.assertNull(compositeMapView.get("notKey"));
    }

    @Test
    void testKeySet() {

        val expectedKeySet = Sets.newHashSet(Arrays.asList("k1", "k2", "k3", "kk2", "kk3"));
        Assertions.assertEquals(expectedKeySet, compositeMapView.keySet());
    }

    @Test
    void testValues() {

        val expectedValueSet = Sets.newHashSet(Arrays.asList("v1", "v2", "v3", "vv1", "vv2", "v3"));
        Assertions.assertTrue(expectedValueSet.containsAll(compositeMapView.values()));
        Assertions.assertEquals(expectedValueSet.size(), compositeMapView.values().size());
    }

    @Test
    void testEntrySet() {

        val entryList = compositeMapView.entrySet().stream().sorted(Map.Entry.comparingByKey(String::compareTo))
                .map(e -> e.getKey() + "," + e.getValue()).collect(Collectors.toList());
        val expectedEntryList = Arrays.asList("k1,vv1", "k2,v2", "k3,v3", "kk2,vv2", "kk3,v3");

        Assertions.assertEquals(expectedEntryList, entryList);
    }

    @Test
    void testNotSupport() {

        val emptyMap = Collections.emptyMap();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.put("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.putIfAbsent("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.remove("a"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.remove("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.putAll(emptyMap));
        Assertions.assertThrows(UnsupportedOperationException.class, compositeMapView::clear);
        Assertions.assertThrows(UnsupportedOperationException.class, compositeMapView::toString);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.forEach((k, v) -> {
        }));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.replaceAll((k, v) -> ""));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.replace("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.replace("a", "b", "b2"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> compositeMapView.computeIfAbsent("a", (k) -> ""));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> compositeMapView.computeIfPresent("a", (k, v) -> ""));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> compositeMapView.compute("a", (k, v) -> ""));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> compositeMapView.merge("a", "b", (k, v) -> ""));

    }
}
