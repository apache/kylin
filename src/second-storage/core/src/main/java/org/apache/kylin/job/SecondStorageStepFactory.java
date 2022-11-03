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

package org.apache.kylin.job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kylin.job.execution.AbstractExecutable;

public class SecondStorageStepFactory {
    private static final Map<Class<? extends SecondStorageStep>, Supplier<AbstractExecutable>> SUPPLIER_MAP = new ConcurrentHashMap<>(5);

    public static void register(Class<? extends SecondStorageStep> stepType, Supplier<AbstractExecutable> stepSupplier) {
        SUPPLIER_MAP.put(stepType, stepSupplier);
    }
    public static AbstractExecutable create(Class<? extends SecondStorageStep> stepType, Consumer<AbstractExecutable> paramInjector) {
        AbstractExecutable step = SUPPLIER_MAP.get(stepType).get();
        paramInjector.accept(step);
        return step;
    }

    public interface SecondStorageStep {
    }

    public interface SecondStorageLoadStep extends SecondStorageStep {

    }

    public interface SecondStorageRefreshStep extends SecondStorageStep {}

    public interface SecondStorageMergeStep extends SecondStorageStep {}

    public interface SecondStorageIndexClean extends SecondStorageStep {}

}
