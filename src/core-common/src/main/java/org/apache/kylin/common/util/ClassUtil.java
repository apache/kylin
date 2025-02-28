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
import java.util.Objects;

import org.apache.commons.lang3.Validate;
import org.slf4j.LoggerFactory;

/**
 */
public class ClassUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ClassUtil.class);

    public static void addClasspath(String path) {
        addToClasspath(path, ClassLoader.getSystemClassLoader());
    }

    public static void addToClasspath(String path, ClassLoader classLoader) {
        logger.info("Adding path " + path + " to class path");
        String[] paths = path.split("[,:]");

        for (String p : paths) {
            File file = new File(p);
            if (file.exists()) {
                _addToClasspath(file, classLoader);
            } else {
                File dir = file.getParentFile();
                Validate.isTrue(dir.exists());
                String match = file.getName();
                Validate.isTrue(match.startsWith("*") && match.endsWith("*"));
                match = match.substring(1, match.length() - 1);
                for (File f : Objects.requireNonNull(dir.listFiles())) {
                    if (f.getName().contains(match) && f.getName().endsWith(".jar"))
                        _addToClasspath(f, classLoader);
                }
            }
        }
    }

    private static void _addToClasspath(File file, ClassLoader classLoader) {
        try {
            if (file.exists()) {
                logger.info("Adding path {} to class path", file);
                Class<URLClassLoader> urlClass = URLClassLoader.class;
                Method method = urlClass.getDeclaredMethod("addURL", URL.class);
                Unsafe.changeAccessibleObject(method, true);
                method.invoke(classLoader, file.toURI().toURL());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Class<? extends T> forName(String name, Class<T> clz) throws ClassNotFoundException {
        return (Class<? extends T>) Class.forName(name);
    }

    public static Object newInstance(String clz) {
        try {
            return forName(clz, Object.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String findContainingJar(Class<?> clazz) {
        return findContainingJar(clazz, null);
    }

    /**
     * Load the first jar library contains clazz with preferJarKeyword matched.
     * If preferJarKeyword is null, just load the
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
                if (preferJarKeyWord != null && url.getPath().contains(preferJarKeyWord))
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
}
