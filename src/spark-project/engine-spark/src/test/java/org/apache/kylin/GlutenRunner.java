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

package org.apache.kylin;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

import scala.Option;

public class GlutenRunner extends BlockJUnit4ClassRunner {

    public GlutenRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    protected GlutenRunner(TestClass testClass) throws InitializationError {
        super(testClass);
    }

    public static void disableGluten() {
        Option<SparkSession> sparkSession = SparkSession.getActiveSession();
        if (sparkSession.isDefined()) {
            sparkSession.get().conf().set("spark.gluten.enabled", "false");
        }
    }

    public static void enableGluten() {
        Option<SparkSession> sparkSession = SparkSession.getActiveSession();
        if (sparkSession.isDefined()) {
            sparkSession.get().conf().set("spark.gluten.enabled", "true");
        }
    }

    @Override
    protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
        List<FrameworkMethod> befores = Lists.newArrayList();
        befores.addAll(getTestClass().getAnnotatedMethods(Before.class));
        if (method.getAnnotation(GlutenDisabled.class) != null) {
            try {
                Method before = GlutenRunner.class.getMethod("disableGluten");
                befores.add(new FrameworkMethod(before));
            } catch (NoSuchMethodException ignored) {
            }
        }
        return befores.isEmpty() ? statement : new RunBefores(statement, befores, target);
    }

    @Override
    protected Statement withAfters(FrameworkMethod method, Object target, Statement statement) {
        List<FrameworkMethod> afters = Lists.newArrayList();
        afters.addAll(getTestClass().getAnnotatedMethods(After.class));
        if (method.getAnnotation(GlutenDisabled.class) != null) {
            try {
                Method after = GlutenRunner.class.getMethod("enableGluten");
                afters.add(new FrameworkMethod(after));
            } catch (NoSuchMethodException ignored) {
            }
        }
        return afters.isEmpty() ? statement : new RunAfters(statement, afters, target);
    }
}
