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

import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ValueIterators {

    public static <T extends Number> long sum(Iterator<T> iterator) {
        long ret = 0;
        while (iterator.hasNext()) {
            Number element = iterator.next();
            ret += element.longValue();
        }
        return ret;
    }

    public static <T> T checkSame(Iterator<T> iterator) {
        Set<T> values = Sets.newHashSet();
        while (iterator.hasNext()) {
            T element = iterator.next();
            values.add(element);
        }

        if (values.size() > 1) {
            throw new IllegalStateException("more than one distinct values exist in the collection:" + values);
        }

        return values.iterator().next();
    }
}
