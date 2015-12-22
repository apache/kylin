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

package org.apache.kylin.metadata.filter.util;

import java.lang.reflect.Method;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.runtime.SqlFunctions;

import com.google.common.collect.ImmutableMap;

/**
 * Created by dongli on 11/13/15.
 */
public enum BuiltInMethod {
    UPPER(SqlFunctions.class, "upper", String.class),
    LOWER(SqlFunctions.class, "lower", String.class),
    SUBSTRING(SqlFunctions.class, "substring", String.class, int.class, int.class),
    CHAR_LENGTH(SqlFunctions.class, "charLength", String.class),
    LIKE(SqlFunctions.class, "like", String.class, String.class),
    INITCAP(SqlFunctions.class, "initcap", String.class);
    public final Method method;

    public static final ImmutableMap<String, BuiltInMethod> MAP;

    static {
        final ImmutableMap.Builder<String, BuiltInMethod> builder =
                ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.name(), value);
            }
        }
        MAP = builder.build();
    }

    BuiltInMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
