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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 */
public class ClassUtil {

    public static void addClasspath(String path) {
        System.out.println("Adding path " + path + " to class path");
        File file = new File(path);

        try {
            if (file.exists()) {
                URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                Class<URLClassLoader> urlClass = URLClassLoader.class;
                Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
                method.setAccessible(true);
                method.invoke(urlClassLoader, new Object[] { file.toURI().toURL() });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final WeakHashMap<String, Class<?>> forNameCache = new WeakHashMap<>();
    private static final Map<String, String> classRenameMap;
    static {
        classRenameMap = new HashMap<>();
        classRenameMap.put("org.apache.kylin.job.common.HadoopShellExecutable", "org.apache.kylin.engine.mr.common.HadoopShellExecutable");
        classRenameMap.put("org.apache.kylin.job.common.MapReduceExecutable", "org.apache.kylin.engine.mr.common.MapReduceExecutable");
        classRenameMap.put("org.apache.kylin.job.cube.CubingJob", "org.apache.kylin.engine.mr.CubingJob");
        classRenameMap.put("org.apache.kylin.job.cube.GarbageCollectionStep", "org.apache.kylin.storage.hbase.steps.DeprecatedGCStep");
        classRenameMap.put("org.apache.kylin.job.cube.MergeDictionaryStep", "org.apache.kylin.engine.mr.steps.MergeDictionaryStep");
        classRenameMap.put("org.apache.kylin.job.cube.UpdateCubeInfoAfterBuildStep", "org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep");
        classRenameMap.put("org.apache.kylin.job.cube.UpdateCubeInfoAfterMergeStep", "org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterMergeStep");
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<? extends T> forName(String name, Class<T> clz) throws ClassNotFoundException {
        String origName = name;

        Class<? extends T> result = (Class<? extends T>) forNameCache.get(origName);
        if (result == null) {
            name = forRenamedClass(name);
            result = (Class<? extends T>) Class.forName(name);
            forNameCache.put(origName, result);
        }
        return result;
    }

    private static String forRenamedClass(String name) {
        if (name.startsWith("com.kylinolap")) {
            name = "org.apache.kylin" + name.substring("com.kylinolap".length());
        }
        String rename = classRenameMap.get(name);
        return rename == null ? name : rename;
    }

    public static Object newInstance(String clz) {
        try {
            return forName(clz, Object.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
