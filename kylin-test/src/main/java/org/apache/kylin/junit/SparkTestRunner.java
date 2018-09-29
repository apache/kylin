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

import org.apache.kylin.ext.ItClassLoader;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.io.IOException;

public class SparkTestRunner extends BlockJUnit4ClassRunner {

    static public ItClassLoader customClassLoader;

    public SparkTestRunner(Class<?> clazz) throws Exception {
        super(loadFromCustomClassloader(clazz));
    }

    // Loads a class in the custom classloader
    private static Class<?> loadFromCustomClassloader(Class<?> clazz) throws Exception {
        if(!EnvUtils.checkEnv("SPARK_HOME")){
            EnvUtils.setNormalEnv();
        }
        try {
            // Only load once to support parallel tests
            if (customClassLoader == null) {
                customClassLoader = new ItClassLoader(Thread.currentThread().getContextClassLoader());
            }
            return Class.forName(clazz.getName(), false, customClassLoader);
        } catch (ClassNotFoundException | IOException e) {
            throw new InitializationError(e);
        }
    }

    public static ItClassLoader get() throws IOException {
        if (customClassLoader == null) {
            customClassLoader = new ItClassLoader(Thread.currentThread().getContextClassLoader());
        }
        return customClassLoader;
    }

    // Runs junit tests in a separate thread using the custom class loader
    @Override
    public void run(final RunNotifier notifier) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                SparkTestRunner.super.run(notifier);
            }
        };
        Thread thread = new Thread(runnable);
        thread.setContextClassLoader(customClassLoader);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}