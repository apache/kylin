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
package org.apache.kylin.cache.softaffinity;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SoftAffinityBookKeeping {

    private static final ThreadLocal<LinkedHashMap<String, String>> THREAD = new ThreadLocal<>();
    private static final int MAX_ENTRIES = 20;

    private SoftAffinityBookKeeping() {
    }

    public static void recordAsk(String file, String[] locations) {
        LinkedHashMap<String, String> book = getBook();
        String loc = String.join("|", locations);
        String existing = book.putIfAbsent(file, loc);
        if (existing != null && !existing.contains(loc))
            book.put(file, existing + "|" + loc);
    }

    @NotNull
    private static LinkedHashMap<String, String> getBook() {
        LinkedHashMap<String, String> book = THREAD.get();
        if (book == null) {
            book = new LinkedHashMap<String, String>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > MAX_ENTRIES;
                }
            };
            THREAD.set(book);
        }
        return book;
    }

    // returns (file -> locations)
    public static Map<String, String> audit() {
        LinkedHashMap<String, String> book = getBook();
        Map<String, String> ret = ImmutableMap.copyOf(book);
        book.clear();
        return ret;
    }

    public static void logAudits() {
        List<String> lines = audit().entrySet().stream().map(e -> e.getKey() + " -> " + e.getValue())
                .collect(Collectors.toList());
        log.debug("Past few mappings of location -> executors: \n{}", String.join("\n", lines));
    }

    public static int size() {
        LinkedHashMap<String, String> book = THREAD.get();
        return book.size();
    }
}
