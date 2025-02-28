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

package org.apache.kylin.query.engine;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.udf.UdfDef;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;
import org.apache.kylin.measure.MeasureTypeFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry for all UDFs.
 * Currently, it just works as a wrapper for udf registered in kylinConfig and MeasureTypeFactory
 */
@Slf4j
public class UdfRegistry {
    private UdfRegistry() {
    }

    public static final HashMultimap<String, Function> allUdfMap = genAllUdf();

    @Getter
    public static class UdfDefinition {
        String name;
        String className;
        String methodName;
        List<String> paths = null;

        public UdfDefinition(String name, String className) {
            this(name, className, "*");
        }

        public UdfDefinition(String name, String className, String methodName) {
            this.name = name;
            this.className = className;
            this.methodName = methodName;
        }

    }

    private static HashMultimap<String, Function> genAllUdf() {
        HashMultimap<String, Function> allUdfMap = HashMultimap.create();

        //udf
        Map<String, UdfDefinition> udfDefinitions = new HashMap<>();
        KylinConfig.getInstanceFromEnv().getUDFs().forEach((udfType, udfDefClazz) -> udfDefinitions.put(udfType,
                new UdfDefinition(udfType.trim().toUpperCase(Locale.ROOT), udfDefClazz.trim())));
        Collections.unmodifiableCollection(udfDefinitions.values()).forEach(udfDef -> {
            try {
                Class<?> clazz = Class.forName(udfDef.getClassName());
                // register functions without implementation
                for (Method method : clazz.getMethods()) {
                    if (method.getDeclaringClass() == Object.class) {
                        continue;
                    }
                    final ScalarFunction function = ScalarFunctionImpl.create(method);
                    allUdfMap.put(method.getName(), function);
                }
                // register function with implementation, the definitions are in KylinOtherUdf
                registerFunctionsWithImpl(clazz);
            } catch (Exception e) {
                log.error("Register UDF {} fail", udfDef.getClassName(), e);
            }
        });

        //udaf
        Map<String, UdfDefinition> udafDefinitions = new HashMap<>();
        for (Map.Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udafDefinitions.put(entry.getKey(), new UdfDefinition(entry.getKey().trim().toUpperCase(Locale.ROOT),
                    entry.getValue().getName().trim(), null));
        }
        for (UdfDefinition udfDef : Collections.unmodifiableCollection(udafDefinitions.values())) {
            try {
                final AggregateFunction aggFunction = AggregateFunctionImpl
                        .create(Class.forName(udfDef.getClassName()));
                allUdfMap.put(udfDef.getName(), aggFunction);
            } catch (ClassNotFoundException e) {
                throw new KylinRuntimeException("UDAF class '" + udfDef.getClassName() + "' not found");
            }
        }

        return allUdfMap;
    }

    private static void registerFunctionsWithImpl(Class<?> clazz) throws IllegalAccessException, NoSuchFieldException {
        for (Class<?> udfClass : clazz.getClasses()) {
            if (UdfDef.class.isAssignableFrom(udfClass)) {
                SqlOperator udfOp = (SqlOperator) udfClass.getField("OPERATOR").get(null);
                CallImplementor udfImpl = (CallImplementor) udfClass.getField("IMPLEMENTOR").get(null);
                SqlStdOperatorTable.instance().register(udfOp);
                RexImpTable.INSTANCE.defineImplementor(udfOp, udfImpl);
            }
        }
    }
}
