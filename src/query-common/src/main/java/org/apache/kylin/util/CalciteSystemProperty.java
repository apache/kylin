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
package org.apache.kylin.util;

/**
 * Calcite introduce CalciteSystemProperty in the new version.
 *
 *  Use this to simplify upgrading
 *
 * @param <T> the type of the property value
 */
public class CalciteSystemProperty<T> {

    /**
     * Whether to run Calcite in debug mode.
     *
     * <p>When debug mode is activated significantly more information is gathered and printed to
     * STDOUT. It is most commonly used to print and identify problems in generated java code. Debug
     * mode is also used to perform more verifications at runtime, which are not performed during
     * normal execution.</p>
     */
    public static final CalciteSystemProperty<Boolean> DEBUG = new CalciteSystemProperty<>(
            Boolean.parseBoolean(System.getProperty("calcite.debug", "FALSE")));

    private final T value;

    private CalciteSystemProperty(T v) {
        this.value = v;
    }

    /**
     * Returns the value of this property.
     *
     * @return the value of this property or <code>null</code> if a default value has not been
     * defined for this property.
     */
    public T value() {
        return value;
    }
}
