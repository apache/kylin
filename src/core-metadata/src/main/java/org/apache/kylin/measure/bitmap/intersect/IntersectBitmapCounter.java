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

package org.apache.kylin.measure.bitmap.intersect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;

/**
 * @Deprecated This class will no longer be used， but leave it in order to make the framework work.
 */
@Deprecated
public class IntersectBitmapCounter {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    public IntersectBitmapCounter(BitmapCounter bitmapCounter) {

    }

    Map<Object, BitmapCounter> map;
    List<String> keyList;

    public List<String> getKeyList() {
        return keyList;
    }

    public void setKeyList(List<String> keyList) {
        this.keyList = keyList;
    }

    public IntersectBitmapCounter() {
        map = new LinkedHashMap<>();
    }

    public static IntersectBitmapCounter wrap(Object key, List<String> keyList, Object value) {
        IntersectBitmapCounter intersectBitmapCounter = new IntersectBitmapCounter();
        intersectBitmapCounter.add(key, keyList, value);
        return intersectBitmapCounter;
    }

    public void add(Object key, List keyList, Object value) {
        if (this.keyList == null) {
            this.keyList = keyList;
        }
        if (this.keyList != null && this.keyList.contains(key)) {
            BitmapCounter counter = null;
            if (map.containsKey(key)) {
                counter = map.get(key);
            }
            if (counter == null) {
                counter = factory.newBitmap();
                map.put(key, counter);
            }
            counter.orWith((BitmapCounter) value);
        }
    }

    public long result() {
        if (keyList == null || keyList.isEmpty()) {
            return 0;
        }
        // if any specified key not in map, the intersection must be 0
        for (Object key : keyList) {
            if (!map.containsKey(key)) {
                return 0;
            }
        }
        BitmapCounter counter = null;
        for (Object key : keyList) {
            BitmapCounter c = map.get(key);
            if (counter == null) {
                counter = factory.newBitmap();
                counter.orWith(c);
            } else {
                counter.andWith(c);
            }
        }
        if (counter == null) {
            return 0;
        } else {
            return counter.getCount();
        }
    }

    public void merge(IntersectBitmapCounter other) {
        Map<Object, BitmapCounter> otherMap = other.getMap();
        for (String key : keyList) {
            if (otherMap.containsKey(key)) {
                if (map.containsKey(key)) {
                    map.get(key).orWith(otherMap.get(key));
                } else {
                    map.put(key, otherMap.get(key));
                }
            }
        }
    }

    public Map<Object, BitmapCounter> getMap() {
        return map;
    }

    public void setMap(Map<Object, BitmapCounter> map) {
        this.map = map;
    }
}
