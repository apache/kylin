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

package org.apache.kylin.spark.classloader;

import java.io.File;
import java.net.URLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClassLoaderUtils {
    static URLClassLoader originClassLoader = null;
    private static Logger logger = LoggerFactory.getLogger(ClassLoaderUtils.class);

    public static File findFile(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().matches(ptn)) {
                    return f;
                }
            }
        }
        return null;
    }

    public static ClassLoader getOriginClassLoader() {
        if (originClassLoader == null) {
            logger.error("originClassLoader not init");
            return Thread.currentThread().getContextClassLoader();
        } else {
            return originClassLoader;
        }
    }

    public static void setOriginClassLoader(URLClassLoader classLoader) {
        if (originClassLoader != null) {
            logger.error("originClassLoader already initialized");
        }
        logger.info("set originClassLoader :" + classLoader);
        originClassLoader = classLoader;
    }
}
