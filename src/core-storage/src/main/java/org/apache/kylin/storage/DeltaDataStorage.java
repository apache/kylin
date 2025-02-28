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
package org.apache.kylin.storage;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.realization.IRealization;

public class DeltaDataStorage implements IStorage {

    @Override
    public IStorageQuery createQuery(IRealization realization) {
        throw new UnsupportedOperationException();
    }

    // Problem 1: Hardcoded Class Names
    public static final String CUBING_ENGINE_CLASS = "org.apache.kylin.engine.spark.NSparkCubingEngine$NSparkCubingStorage";
    public static final String DELTA_STORAGE_CLASS = "org.apache.kylin.engine.spark.storage.DeltaStorage";

    // Define custom exceptions
    class ClassLoadingException extends RuntimeException {
        ClassLoadingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    class InstantiationException extends RuntimeException {
        InstantiationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    class AdaptationException extends RuntimeException {
        AdaptationException(String message) {
            super(message);
        }
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        Class<I> clz;
        try {
            clz = (Class<I>) Class.forName(CUBING_ENGINE_CLASS);
        } catch (ClassNotFoundException e) {
            throw new ClassLoadingException("Class not found: " + CUBING_ENGINE_CLASS, e);
        }
        if (engineInterface == clz) {
            try {
                return (I) ClassUtil.newInstance(DELTA_STORAGE_CLASS);
            } catch (Exception e) {
                throw new InstantiationException("Cannot instantiate class: " + DELTA_STORAGE_CLASS, e);
            }
        } else {
            throw new AdaptationException("Cannot adapt to " + engineInterface);
        }
    }
}
