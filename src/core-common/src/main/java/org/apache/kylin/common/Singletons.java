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
package org.apache.kylin.common;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.Unsafe;

import lombok.val;

public class Singletons implements Serializable {

    transient ConcurrentHashMap<Class<?>, Object> instances = null;
    transient ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, Object>> instancesByPrj = null;

    public static <T> T getInstance(String project, Class<T> clz) {
        return instance.getInstance0(project, clz, defaultCreator(project));
    }

    public static <T> T getInstance(Class<T> clz) {
        return instance.getInstance0(clz, defaultCreator());
    }

    public static <T> T getInstance(String project, Class<T> clz, Creator<T> creator) {
        return instance.getInstance0(project, clz, creator);
    }

    public static <T> T getInstance(Class<T> clz, Creator<T> creator) {
        return instance.getInstance0(clz, creator);
    }

    public static void clearInstance(Class<?> tClass) {
        instance.clearByType(tClass);
    }

    static <T> Creator<T> defaultCreator(String project) {
        return clz -> {
            Constructor<T> method = clz.getDeclaredConstructor(String.class);
            Unsafe.changeAccessibleObject(method, true);
            return method.newInstance(project);
        };
    }

    static <T> Creator<T> defaultCreator() {
        return clz -> {
            Constructor<T> method = clz.getDeclaredConstructor();
            Unsafe.changeAccessibleObject(method, true);
            return method.newInstance();
        };
    }

    private static final Singletons instance = new Singletons();

    Singletons() {
    }

    <T> T getInstance0(Class<T> clz, Creator<T> creator) {
        Object singleton = instances == null ? null : instances.get(clz);
        if (singleton != null)
            return (T) singleton;

        synchronized (this) {
            if (instances == null)
                instances = new ConcurrentHashMap<>();

            singleton = instances.get(clz);
            if (singleton != null)
                return (T) singleton;

            try {
                singleton = creator.create(clz);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (singleton != null) {
                instances.put(clz, singleton);
            }
        }
        return (T) singleton;
    }

    <T> T getInstance0(String project, Class<T> clz, Creator<T> creator) {
        ConcurrentHashMap<String, Object> instanceMap = (null == instancesByPrj) ? null : instancesByPrj.get(clz);
        Object singleton = (null == instanceMap) ? null : instanceMap.get(project);
        if (singleton != null)
            return (T) singleton;

        synchronized (this) {
            if (null == instancesByPrj)
                instancesByPrj = new ConcurrentHashMap<>();

            instanceMap = instancesByPrj.get(clz);
            if (instanceMap == null)
                instanceMap = new ConcurrentHashMap<>();

            singleton = instanceMap.get(project);
            if (singleton != null)
                return (T) singleton;

            try {
                singleton = creator.create(clz);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (singleton != null) {
                instanceMap.put(project, singleton);
            }
            instancesByPrj.put(clz, instanceMap);
        }
        return (T) singleton;
    }

    void clear() {
        if (instances != null)
            instances.clear();
    }

    void clearByProject(String project) {
        if (instancesByPrj != null) {
            for (val value : instancesByPrj.values()) {
                value.remove(project);
            }
        }
    }

    void clearByType(Class<?> clz) {
        if (instances != null)
            instances.remove(clz);
    }

    public interface Creator<T> {
        T create(Class<T> clz) throws Exception;
    }
}
