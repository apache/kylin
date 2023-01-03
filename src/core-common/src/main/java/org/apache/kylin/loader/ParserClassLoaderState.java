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

package org.apache.kylin.loader;

import java.security.AccessController;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.loader.utils.ClassLoaderUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * A project a singleton object
 * Manage classloader of resolver in project
 */
@Slf4j
public class ParserClassLoaderState {

    private final String project;

    private Set<String> loadedJars = Sets.newCopyOnWriteArraySet();

    private volatile ClassLoader classLoader;

    private static final Map<String, ParserClassLoaderState> instanceMap = Maps.newConcurrentMap();

    private ParserClassLoaderState(String project) {
        this.project = project;
        // init class loader
        ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
        AddToClassPathAction action = new AddToClassPathAction(parentLoader, Collections.emptyList(), true);
        final ParserClassLoader parserClassLoader = AccessController.doPrivileged(action);
        setClassLoader(parserClassLoader);
    }

    public static ParserClassLoaderState getInstance(String project) {
        if (instanceMap.getOrDefault(project, null) == null) {
            synchronized (ParserClassLoaderState.class) {
                if (instanceMap.getOrDefault(project, null) == null) {
                    instanceMap.put(project, new ParserClassLoaderState(project));
                }
            }
        }
        return instanceMap.get(project);
    }

    public void registerJars(Set<String> newJars) {
        try {
            if (ClassLoaderUtils.judgeIntersection(loadedJars, newJars)) {
                throw new IllegalArgumentException("There is already a jar to load " + newJars
                        + ", please ensure that the jar will not be loaded twice");
            }
            AddToClassPathAction addAction = new AddToClassPathAction(getClassLoader(), newJars);
            final ParserClassLoader parserClassLoader = AccessController.doPrivileged(addAction);
            loadedJars.addAll(newJars);
            setClassLoader(parserClassLoader);
            log.info("Load Jars: {}", newJars);
        } catch (Exception e) {
            loadedJars.removeAll(newJars);
            throw new IllegalArgumentException("Unable to register: " + newJars, e);
        }
    }

    public void unregisterJar(Set<String> unregisterJars) {
        try {
            ClassLoaderUtils.removeFromClassPath(project, unregisterJars.toArray(new String[0]), getClassLoader());
            loadedJars.removeAll(unregisterJars);
            log.info("Unload Jars: {}", unregisterJars);
        } catch (Exception e) {
            log.error("Unable to unregister {}", unregisterJars, e);
            throw new IllegalArgumentException("Unable to unregister: " + unregisterJars, e);
        }
    }

    public Set<String> getLoadedJars() {
        return loadedJars;
    }

    public void setLoadedJars(Set<String> loadedJars) {
        this.loadedJars = loadedJars;
    }

    public synchronized ClassLoader getClassLoader() {
        return classLoader;
    }

    public synchronized void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}
