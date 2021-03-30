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

import java.util.Collection;
import java.util.IdentityHashMap;

/**
 */
public class IdentityUtils {

    private IdentityUtils() {
        throw new IllegalStateException("Class IdentityUtils is an utility class !");
    }

    public static <K> boolean collectionReferenceEquals(Collection<K> collectionA, Collection<K> collectionB) {
        if (collectionA == null || collectionB == null) {
            throw new RuntimeException("input must be not null");
        }

        IdentityHashMap<K, Void> mapA = new IdentityHashMap<>();
        IdentityHashMap<K, Void> mapB = new IdentityHashMap<>();
        for (K key : collectionA) {
            mapA.put(key, null);
        }
        for (K key : collectionB) {
            mapB.put(key, null);
        }

        if (mapA.keySet().size() != mapB.keySet().size()) {
            return false;
        }

        for (K key : mapA.keySet()) {
            if (!mapB.keySet().contains(key)) {
                return false;
            }
        }
        return true;
    }
}
