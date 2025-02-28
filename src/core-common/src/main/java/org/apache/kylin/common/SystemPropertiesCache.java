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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * It's a workaround to avoid lock in bottom hash table
 * It can be removed after updating JDK to 11
 */
public class SystemPropertiesCache {

    private static final ConcurrentHashMap<Object, Object> CACHED_SYSTEM_PROPERTY = new ConcurrentHashMap<>(
            System.getProperties());

    protected static Map<Object, Object> getProperties() {
        return CACHED_SYSTEM_PROPERTY;
    }

    protected static String getProperty(String key) {
        checkKey(key);
        Object oval = CACHED_SYSTEM_PROPERTY.get(key);
        return (oval instanceof String) ? (String) oval : null;
    }

    protected static String getProperty(String key, String defaultValue) {
        String val = getProperty(key);
        return (val == null) ? defaultValue : val;
    }

    // Mainly invoked in tests
    public static String setProperty(String key, String value) {
        System.setProperty(key, value);
        return (String) CACHED_SYSTEM_PROPERTY.put(key, value);
    }

    // Mainly invoked in tests
    public static void clearProperty(String key) {
        System.clearProperty(key);
        CACHED_SYSTEM_PROPERTY.remove(key);
    }

    private static void checkKey(String key) {
        if (StringUtils.isEmpty(key)) {
            throw new IllegalArgumentException("Property key can't be null");
        }
    }
}
