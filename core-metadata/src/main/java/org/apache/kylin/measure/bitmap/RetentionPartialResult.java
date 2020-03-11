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
package org.apache.kylin.measure.bitmap;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RetentionPartialResult {

    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    public static final String FILTER_DELIMETER = KylinConfig.getInstanceFromEnv().getIntersectFilterOrSeparator();
    Map<String, BitmapCounter> map;
    List<String> keyList;
    Map<String, List<String>> childKeyToParentKey;

    public RetentionPartialResult() {
        map = new LinkedHashMap<>();
    }

    public void add(Object key, List keyList, Object value) {
        Preconditions.checkArgument(key != null);
        Preconditions.checkArgument(keyList != null && keyList.size() >= 0);
        if (this.keyList == null) {
            this.keyList = Lists.transform(keyList, i -> i.toString());
            childKeyToParentKey = new HashMap<>(5);

            for (String sKey : this.keyList) {
                String[] elements = StringUtil.splitAndTrim(sKey, FILTER_DELIMETER);
                for (String s : elements) {
                    if (s != null && s.trim().length() > 0) {
                        List<String> parent = childKeyToParentKey.computeIfAbsent(s.trim(), o -> new ArrayList());
                        parent.add(sKey);
                    }
                }
            }

        }

        if (this.keyList != null) {
            if (this.keyList.contains(key.toString())) {
                BitmapCounter counter = map.computeIfAbsent(key.toString(), o -> factory.newBitmap());
                counter.orWith((BitmapCounter) value);
            }

            if (childKeyToParentKey.size() > 0) {
                String sKey = key.toString();
                if (childKeyToParentKey.containsKey(sKey)) {
                    List<String> parents = childKeyToParentKey.get(sKey);
                    for (String parent : parents) {
                        BitmapCounter counter = map.computeIfAbsent(parent, o -> factory.newBitmap());
                        counter.orWith((BitmapCounter) value);
                    }
                }
            }
        }
    }

    private BitmapCounter result() {
        if (keyList == null || keyList.isEmpty()) {
            return null;
        }
        // if any specified key not in map, the intersection must be 0
        for (String key : keyList) {
            if (!map.containsKey(key)) {
                return null;
            }
        }
        BitmapCounter counter = null;
        for (String key : keyList) {
            BitmapCounter c = map.get(key);
            if (counter == null) {
                counter = factory.newBitmap();
                counter.orWith(c);
            } else {
                counter.andWith(c);
            }
        }

        return counter;
    }

    public String valueResult() {
        BitmapCounter counter = result();
        String result = "";
        if (counter != null && counter.getCount() > 0) {
            result = "[" + StringUtils.join(counter.iterator(), ",") + "]";
        }
        return result;
    }

    public long countResult() {
        BitmapCounter counter = result();
        return counter != null ? counter.getCount() : 0;
    }

}