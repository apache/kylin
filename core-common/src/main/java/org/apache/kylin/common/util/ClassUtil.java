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
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

/**
 */
public class ClassUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ClassUtil.class);

    private ClassUtil() {
        throw new IllegalStateException("Class ClassUtil is an utility class !");
    }

    public static void addClasspath(String path) {
        logger.info("Adding path " + path + " to class path");
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
        classRenameMap.put("org.apache.kylin.rest.util.KeywordDefaultDirtyHack", "org.apache.kylin.query.util.KeywordDefaultDirtyHack");
    }

    public static <T> Class<? extends T> forName(String name, Class<T> clz) throws ClassNotFoundException {
        name = forRenamedClass(name);
        return (Class<? extends T>) Class.forName(name);
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
            return forName(clz, Object.class).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String findContainingJar(Class<?> clazz) {
        return findContainingJar(clazz, null);
    }

    /**
     * Load the first jar library contains clazz with preferJarKeyword matched. If preferJarKeyword is null, just load the
     * jar likes Hadoop Commons' ClassUtil
     * @param clazz
     * @param preferJarKeyWord
     * @return
     */
    public static String findContainingJar(Class<?> clazz, String preferJarKeyWord) {
        ClassLoader loader = clazz.getClassLoader();
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";

        try {
            Enumeration e = loader.getResources(classFile);

            URL url = null;
            do {
                if (!e.hasMoreElements()) {
                    if (url == null)
                        return null;
                    else
                        break;
                }

                url = (URL) e.nextElement();
                if (!"jar".equals(url.getProtocol()))
                    break;
                if (preferJarKeyWord != null && url.getPath().indexOf(preferJarKeyWord) != -1)
                    break;
                if (preferJarKeyWord == null)
                    break;
            } while (true);

            String toReturn = url.getPath();
            if (toReturn.startsWith("file:")) {
                toReturn = toReturn.substring("file:".length());
            }

            toReturn = URLDecoder.decode(toReturn, "UTF-8");
            return toReturn.replaceAll("!.*$", "");
        } catch (IOException var6) {
            throw new RuntimeException(var6);
        }
    }

    public static String findContainingJar(String className, String perferLibraryName) {
        try {
            return findContainingJar(Class.forName(className), perferLibraryName);
        } catch (ClassNotFoundException e) {
            logger.warn("failed to locate jar for class " + className + ", ignore it");
        }

        return "";
    }
}
