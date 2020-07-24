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

import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.cache.Cache;
import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.cache.RemovalListener;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;

public class CacheBuilderTest {
    @Test
    public void foo() {
        Cache<Object, Object> build = CacheBuilder.newBuilder().maximumSize(1).weakValues().removalListener(new RemovalListener<Object, Object>() {
            @Override
            public void onRemoval(RemovalNotification<Object, Object> notification) {
                System.out.println(notification.getCause());
            }
        }).build();

        build.put(1, 1);
        build.put(1, 2);
        build.put(2, 2);

    }
}
