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
package org.apache.kylin.metadata.streaming;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class ReflectionUtils {
    private static final Log logger = LogFactory.getLog(ReflectionUtils.class);

    public static void setField(Object targetObject, String name, Object value) {
        setField((Object) targetObject, name, value, (Class) null);
    }

    public static void setField(Object targetObject, String name, Object value, Class<?> type) {
        setField(targetObject, (Class) null, name, value, type);
    }

    public static void setField(Class<?> targetClass, String name, Object value) {
        setField((Object) null, targetClass, name, value, (Class) null);
    }

    public static void setField(Class<?> targetClass, String name, Object value, Class<?> type) {
        setField((Object) null, targetClass, name, value, type);
    }

    public static void setField(Object targetObject, Class<?> targetClass, String name, Object value, Class<?> type) {
        Assert.isTrue(targetObject != null || targetClass != null,
                "Either targetObject or targetClass for the field must be specified");

        if (targetClass == null) {
            targetClass = targetObject.getClass();
        }

        Field field = org.springframework.util.ReflectionUtils.findField(targetClass, name, type);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' of type [%s] on %s or target class [%s]",
                            name, type, safeToString(targetObject), targetClass));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(Locale.ROOT,
                        "Setting field '%s' of type [%s] on %s or target class [%s] to value [%s]", name, type,
                        safeToString(targetObject), targetClass, value));
            }

            org.springframework.util.ReflectionUtils.makeAccessible(field);
            org.springframework.util.ReflectionUtils.setField(field, targetObject, value);
        }
    }

    public static Object getField(Object targetObject, String name) {
        return getField(targetObject, (Class) null, name);
    }

    public static Object getField(Class<?> targetClass, String name) {
        return getField((Object) null, targetClass, name);
    }

    public static Object getField(Object targetObject, Class<?> targetClass, String name) {
        Assert.isTrue(targetObject != null || targetClass != null,
                "Either targetObject or targetClass for the field must be specified");

        if (targetClass == null) {
            targetClass = targetObject.getClass();
        }

        Field field = org.springframework.util.ReflectionUtils.findField(targetClass, name);
        if (field == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find field '%s' on %s or target class [%s]", name,
                            safeToString(targetObject), targetClass));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(Locale.ROOT, "Getting field '%s' from %s or target class [%s]", name,
                        safeToString(targetObject), targetClass));
            }

            org.springframework.util.ReflectionUtils.makeAccessible(field);
            return org.springframework.util.ReflectionUtils.getField(field, targetObject);
        }
    }

    public static void invokeSetterMethod(Object target, String name, Object value) {
        invokeSetterMethod(target, name, value, (Class) null);
    }

    public static void invokeSetterMethod(Object target, String name, Object value, Class<?> type) {
        Assert.notNull(target, "Target object must not be null");
        Assert.hasText(name, "Method name must not be empty");
        Class<?>[] paramTypes = type != null ? new Class[] { type } : null;
        String setterMethodName = name;
        if (!name.startsWith("set")) {
            setterMethodName = "set" + StringUtils.capitalize(name);
        }

        Method method = org.springframework.util.ReflectionUtils.findMethod(target.getClass(), setterMethodName,
                paramTypes);
        if (method == null && !setterMethodName.equals(name)) {
            setterMethodName = name;
            method = org.springframework.util.ReflectionUtils.findMethod(target.getClass(), name, paramTypes);
        }

        if (method == null) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Could not find setter method '%s' on %s with parameter type [%s]",
                            setterMethodName, safeToString(target), type));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(Locale.ROOT, "Invoking setter method '%s' on %s with value [%s]",
                        setterMethodName, safeToString(target), value));
            }

            org.springframework.util.ReflectionUtils.makeAccessible(method);
            org.springframework.util.ReflectionUtils.invokeMethod(method, target, new Object[] { value });
        }
    }

    public static Object invokeGetterMethod(Object target, String name) {
        Assert.notNull(target, "Target object must not be null");
        Assert.hasText(name, "Method name must not be empty");
        String getterMethodName = name;
        if (!name.startsWith("get")) {
            getterMethodName = "get" + StringUtils.capitalize(name);
        }

        Method method = org.springframework.util.ReflectionUtils.findMethod(target.getClass(), getterMethodName);
        if (method == null && !getterMethodName.equals(name)) {
            getterMethodName = name;
            method = org.springframework.util.ReflectionUtils.findMethod(target.getClass(), name);
        }

        if (method == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Could not find getter method '%s' on %s",
                    getterMethodName, safeToString(target)));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format(Locale.ROOT, "Invoking getter method '%s' on %s", getterMethodName,
                        safeToString(target)));
            }

            org.springframework.util.ReflectionUtils.makeAccessible(method);
            return org.springframework.util.ReflectionUtils.invokeMethod(method, target);
        }
    }

    private static String safeToString(Object target) {
        try {
            return String.format(Locale.ROOT, "target object [%s]", target);
        } catch (Exception var2) {
            return String.format(Locale.ROOT, "target of type [%s] whose toString() method threw [%s]",
                    target != null ? target.getClass().getName() : "unknown", var2);
        }
    }
}
