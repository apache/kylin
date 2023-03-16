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
import java.util.Collection;
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
import org.apache.calcite.sql.fun.udf.UdfEmptyImplementor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;

/**
 * Registry for all UDFs.
 * Currently it just works as a wrapper for udf registered in kylinConfig and MeasureTypeFactory
 * TODO: move udf registration here
 */
public class UDFRegistry {

    private KylinConfig kylinConfig;
    private Map<String, UDFDefinition> udfDefinitions = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(UDFRegistry.class);

    public static UDFRegistry getInstance(KylinConfig config, String project) {
        return config.getManager(project, UDFRegistry.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static UDFRegistry newInstance(KylinConfig conf, String project) {
        try {
            String cls = UDFRegistry.class.getName();
            Class<? extends UDFRegistry> clz = ClassUtil.forName(cls, UDFRegistry.class);
            return clz.getConstructor(KylinConfig.class, String.class).newInstance(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    static HashMultimap<String, Function> allUdfMap = genAllUdf();

    public UDFRegistry(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;

        //udf
        for (Map.Entry<String, String> entry : KylinConfig.getInstanceFromEnv().getUDFs().entrySet()) {
            udfDefinitions.put(entry.getKey(),
                    new UDFDefinition(entry.getKey().trim().toUpperCase(Locale.ROOT), entry.getValue().trim()));
        }
        //udaf
        for (Map.Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udfDefinitions.put(entry.getKey(), new UDFDefinition(entry.getKey().trim().toUpperCase(Locale.ROOT),
                    entry.getValue().getName().trim(), null));
        }
    }

    public Collection<UDFDefinition> getUdfDefinitions() {
        return Collections.unmodifiableCollection(udfDefinitions.values());
    }

    public static class UDFDefinition {
        String name;
        String className;
        String methodName;
        List<String> paths = null;

        public UDFDefinition(String name, String className) {
            this(name, className, "*");
        }

        public UDFDefinition(String name, String className, String methodName) {
            this.name = name;
            this.className = className;
            this.methodName = methodName;
        }

        public String getName() {
            return name;
        }

        public String getClassName() {
            return className;
        }

        public String getMethodName() {
            return methodName;
        }

        public List<String> getPaths() {
            return paths;
        }
    }

    private static HashMultimap<String, Function> genAllUdf() {
        HashMultimap<String, Function> allUdfMap = HashMultimap.create();

        //udf
        Map<String, UDFDefinition> udfDefinitions = new HashMap<>();
        for (Map.Entry<String, String> entry : KylinConfig.getInstanceFromEnv().getUDFs().entrySet()) {
            udfDefinitions.put(entry.getKey(),
                    new UDFDefinition(entry.getKey().trim().toUpperCase(Locale.ROOT), entry.getValue().trim()));
        }
        for (UDFRegistry.UDFDefinition udfDef : Collections.unmodifiableCollection(udfDefinitions.values())) {
            try {
                Class<?> clazz = Class.forName(udfDef.getClassName());
                if (UdfDef.class.isAssignableFrom(clazz)) {
                    SqlOperator udfOp;
                    udfOp = (SqlOperator) clazz.getField("OPERATOR").get(null);

                    CallImplementor udfImpl = UdfEmptyImplementor.INSTANCE;
                    udfImpl = (CallImplementor) clazz.getField("IMPLEMENTOR").get(null);

                    SqlStdOperatorTable.instance().register(udfOp);
                    RexImpTable.INSTANCE.defineImplementor(udfOp, udfImpl);
                    continue;
                }
                for (Method method : clazz.getMethods()) {
                    if (method.getDeclaringClass() == Object.class) {
                        continue;
                    }
                    final ScalarFunction function = ScalarFunctionImpl.create(method);
                    allUdfMap.put(method.getName(), function);
                }
            } catch (Exception e) {
                logger.error("Register UDF {} fail", udfDef.getClassName(), e);
            }
        }

        //udaf
        Map<String, UDFDefinition> udafDefinitions = new HashMap<>();
        for (Map.Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udafDefinitions.put(entry.getKey(), new UDFDefinition(entry.getKey().trim().toUpperCase(Locale.ROOT),
                    entry.getValue().getName().trim(), null));
        }
        for (UDFRegistry.UDFDefinition udfDef : Collections.unmodifiableCollection(udafDefinitions.values())) {
            try {
                final AggregateFunction aggFunction = AggregateFunctionImpl
                        .create(Class.forName(udfDef.getClassName()));
                allUdfMap.put(udfDef.getName(), aggFunction);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("UDAF class '" + udfDef.getClassName() + "' not found");
            }
        }

        return allUdfMap;
    }
}
