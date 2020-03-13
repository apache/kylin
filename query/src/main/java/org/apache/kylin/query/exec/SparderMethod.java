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

package org.apache.kylin.query.exec;

import java.lang.reflect.Method;
import java.util.HashMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Types;

/**
 * Built-in methods in the Spark adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
public enum SparderMethod {
    COLLECT(SparkExec.class, "collectToEnumerable", DataContext.class), //
    COLLECT_SCALAR(SparkExec.class, "collectToScalarEnumerable", DataContext.class),
    ASYNC_RESULT(SparkExec.class, "asyncResult", DataContext.class);

    private static final HashMap<Method, SparderMethod> MAP = new HashMap<Method, SparderMethod>();

    static {
        for (SparderMethod method : SparderMethod.values()) {
            MAP.put(method.method, method);
        }
    }

    public final Method method;

    SparderMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

    public static SparderMethod lookup(Method method) {
        return MAP.get(method);
    }
}

// End SparkMethod.java
