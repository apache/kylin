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

package org.apache.kylin.common.util;

import java.lang.reflect.Method;

import org.apache.kylin.common.KylinConfig;

/**
 * @author ysong1
 *
 */
public abstract class AbstractKylinTestCase {

    public static final String[] SERVICES_WITH_CACHE = { //
            "org.apache.kylin.cube.CubeManager", //
            "org.apache.kylin.cube.CubeDescManager", //
            "org.apache.kylin.storage.hybrid.HybridManager", //
            "org.apache.kylin.metadata.realization.RealizationRegistry", //
            "org.apache.kylin.metadata.project.ProjectManager", //
            "org.apache.kylin.metadata.MetadataManager" //
    };

    public abstract void createTestMetadata() throws Exception;

    public abstract void cleanupTestMetadata() throws Exception;

    public static KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    public static void staticCleanupTestMetadata() {
        cleanupCache();
        System.clearProperty(KylinConfig.KYLIN_CONF);
        KylinConfig.destroyInstance();

    }

    private static void cleanupCache() {

        for (String serviceClass : SERVICES_WITH_CACHE) {
            try {
                Class<?> cls = Class.forName(serviceClass);
                Method method = cls.getDeclaredMethod("clearCache");
                method.invoke(null);
            } catch (ClassNotFoundException e) {
                // acceptable because lower module test does have CubeManager etc on classpath
            } catch (Exception e) {
                System.err.println("Error clean up cache " + serviceClass);
                e.printStackTrace();
            }
        }
    }
}
