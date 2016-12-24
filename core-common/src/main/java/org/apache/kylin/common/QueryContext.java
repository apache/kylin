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

import com.google.common.collect.Maps;

/**
 * checkout {@link org.apache.kylin.common.debug.BackdoorToggles} for comparision
 */
public class QueryContext {
    private static final ThreadLocal<Map<String, String>> _queryContext = new ThreadLocal<Map<String, String>>();

    public final static String KEY_QUERY_ID = "QUERY_ID";

    public static String getQueryId() {
        return getString(KEY_QUERY_ID);
    }

    public static void setQueryId(String uuid) {
        setString(KEY_QUERY_ID, uuid);
    }

    private static void setString(String key, String value) {
        Map<String, String> context = _queryContext.get();
        if (context == null) {
            Map<String, String> newMap = Maps.newHashMap();
            newMap.put(key, value);
            _queryContext.set(newMap);
        } else {
            context.put(key, value);
        }
    }

    private static String getString(String key) {
        Map<String, String> context = _queryContext.get();
        if (context == null) {
            return null;
        } else {
            return context.get(key);
        }
    }

}
