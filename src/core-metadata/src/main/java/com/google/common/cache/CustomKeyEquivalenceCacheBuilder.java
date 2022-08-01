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

package com.google.common.cache;

import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Equivalence;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CustomKeyEquivalenceCacheBuilder {

    private static final Equivalence<Object> KEY_CASE_IGNORE_EQUIVALENCE = new Equivalence<Object>() {
        @Override
        protected boolean doEquivalent(Object a, Object b) {
            if (a instanceof String && b instanceof String) {
                return Objects.equals(((String) a).toLowerCase(Locale.ROOT), ((String) b).toLowerCase(Locale.ROOT));
            }
            return Objects.equals(a, b);
        }

        @Override
        protected int doHash(Object o) {
            if (o instanceof String) {
                return ((String) o).toLowerCase(Locale.ROOT).hashCode();
            }
            return o.hashCode();
        }
    };

    public static CacheBuilder<Object, Object> newBuilder() {
        if (KylinConfig.getInstanceFromEnv().isMetadataKeyCaseInSensitiveEnabled()) {
            return CacheBuilder.newBuilder().keyEquivalence(KEY_CASE_IGNORE_EQUIVALENCE);
        }
        return CacheBuilder.newBuilder().keyEquivalence(Equivalence.equals());
    }

}
