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
package org.apache.kylin.junit;

import java.util.Map;

import org.apache.kylin.common.SystemPropertiesCache;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import lombok.val;

public class OverwritePropExtension implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(OverwritePropExtension.class);
    private static final String OVERWRITE_PROP_BEFORE_ALL_KEY = "OverwriteProp_all";
    private static final String OVERWRITE_PROP_BEFORE_EACH_KEY = "OverwriteProp_each";
    private static final String OVERWRITTEN_PROP_KEY = "OverwrittenProp";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        readFromAnnotation(context, OVERWRITE_PROP_BEFORE_ALL_KEY);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        readFromAnnotation(context, OVERWRITE_PROP_BEFORE_EACH_KEY);
        Map<String, String> overwritten = context.getStore(NAMESPACE).getOrComputeIfAbsent(OVERWRITTEN_PROP_KEY,
                __ -> Maps.newHashMap(), Map.class);
        Map<String, String> props = context.getStore(NAMESPACE).getOrDefault(OVERWRITE_PROP_BEFORE_ALL_KEY, Map.class,
                Maps.newHashMap());
        props.forEach((k, v) -> Unsafe.overwriteSystemProp(overwritten, k, v));
        props = context.getStore(NAMESPACE).getOrDefault(OVERWRITE_PROP_BEFORE_EACH_KEY, Map.class, Maps.newHashMap());
        props.forEach((k, v) -> Unsafe.overwriteSystemProp(overwritten, k, v));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Map<String, String> overwritten = context.getStore(NAMESPACE).getOrComputeIfAbsent(OVERWRITTEN_PROP_KEY,
                __ -> Maps.newHashMap(), Map.class);
        val keys = Sets.newHashSet(overwritten.keySet());
        for (String property : keys) {
            if (!overwritten.containsKey(property) || overwritten.get(property) == null) {
                SystemPropertiesCache.clearProperty(property);
            } else {
                SystemPropertiesCache.setProperty(property, overwritten.get(property));
            }
        }
        context.getStore(NAMESPACE).remove(OVERWRITE_PROP_BEFORE_EACH_KEY);
        context.getStore(NAMESPACE).remove(OVERWRITTEN_PROP_KEY);
    }

    private void readFromAnnotation(ExtensionContext context, String key) {
        AnnotationSupport.findRepeatableAnnotations(context.getElement(), OverwriteProp.class)
                .forEach(x -> context.getStore(NAMESPACE).getOrComputeIfAbsent(key, __ -> Maps.newHashMap(), Map.class)
                        .put(x.key(), x.value()));
    }

    /** Clear system property in test method with annotation {@link org.junit.Test} */
    public final void restoreSystemProp(Map<String, String> overwritten, String property) {
        if (!overwritten.containsKey(property) || overwritten.get(property) == null) {
            SystemPropertiesCache.clearProperty(property);
        } else {
            SystemPropertiesCache.setProperty(property, overwritten.get(property));
        }
        overwritten.remove(property);
    }
}
