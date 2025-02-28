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

package org.apache.kylin.loader.utils;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.kylin.loader.AddToClassPathAction;
import org.apache.kylin.loader.ParserClassLoader;
import org.apache.kylin.loader.ParserClassLoaderState;
import org.springframework.util.ReflectionUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassLoaderUtils {

    private ClassLoaderUtils() {
    }

    public static boolean judgeIntersection(Set<String> set1, Collection<String> collection) {
        Set<String> setCopy = new HashSet<>(set1);
        setCopy.retainAll(collection);
        return !setCopy.isEmpty();
    }

    /**
     * Create a URL from a string representing a path to a local file.
     * The path string can be just a path, or can start with file:/、hdfs:/
     */
    public static URL urlFromPathString(String path) {
        URL resultUrl = null;
        try {
            if (StringUtils.indexOf(path, "file:/") == 0) {
                resultUrl = new URL(path);
            } else if (StringUtils.indexOf(path, "hdfs:/") == 0) {
                registerFactory(new FsUrlStreamHandlerFactory());
                resultUrl = new org.apache.hadoop.fs.Path(path).toUri().toURL();
            }
        } catch (Exception e) {
            log.error("Bad URL {}, ignoring path", path, e);
            throw new IllegalArgumentException("Bad URL [" + path + "], ignoring path");
        }
        if (resultUrl == null) {
            throw new IllegalArgumentException(
                    "URL [" + path + "] not supported, currently only hdfs and file is supported");
        }
        return resultUrl;
    }

    /**
     * URL add hdfs protocol
     */
    @SneakyThrows
    private static void registerFactory(final FsUrlStreamHandlerFactory fsUrlStreamHandlerFactory) {
        log.info("registerFactory : {}", fsUrlStreamHandlerFactory.getClass().getName());
        final Field factoryField = ReflectionUtils.findField(URL.class, "factory");
        ReflectionUtils.makeAccessible(Objects.requireNonNull(factoryField));
        final Field lockField = ReflectionUtils.findField(URL.class, "streamHandlerLock");
        ReflectionUtils.makeAccessible(Objects.requireNonNull(lockField));
        // use same lock as in java.net.URL.setURLStreamHandlerFactory
        synchronized (lockField.get(null)) {
            final URLStreamHandlerFactory originalUrlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField
                    .get(null);
            // Reset the value to prevent Error due to a factory already defined
            ReflectionUtils.setField(factoryField, null, null);
            URL.setURLStreamHandlerFactory(protocol -> {
                if ("hdfs".equals(protocol)) {
                    return fsUrlStreamHandlerFactory.createURLStreamHandler(protocol);
                }
                return originalUrlStreamHandlerFactory.createURLStreamHandler(protocol);
            });
        }
    }

    public static void closeClassLoader(ClassLoader loader) throws IOException {
        if (loader instanceof Closeable) {
            ((Closeable) loader).close();
            return;
        }
        log.warn("Ignoring attempt to close class loader ({}) -- not instance of ParserClassLoader.",
                loader == null ? "mull" : loader.getClass().getSimpleName());
    }

    public static void removeFromClassPath(String project, String[] pathsToRemove, ClassLoader classLoader)
            throws IOException {

        if (!(classLoader instanceof ParserClassLoader)) {
            log.warn(
                    "Ignoring attempt to manipulate {}; probably means we have closed more Parser loaders than opened.",
                    classLoader == null ? "null" : classLoader.getClass().getSimpleName());
            return;
        }

        ParserClassLoader loader = (ParserClassLoader) classLoader;
        List<URL> newPaths = new ArrayList<>(Arrays.asList(loader.getURLs()));
        Arrays.stream(pathsToRemove).forEach(path -> newPaths.remove(urlFromPathString(path)));

        // close old ClassLoader
        closeClassLoader(loader);
        // reload new ClassLoader
        AddToClassPathAction action = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(),
                newPaths.stream().map(URL::toString).collect(Collectors.toSet()));
        final ParserClassLoader parserClassLoader = AccessController.doPrivileged(action);
        // set new ClassLoader
        ParserClassLoaderState.getInstance(project).setClassLoader(parserClassLoader);
    }
}
