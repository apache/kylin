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

package org.apache.kylin.rest.bean;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

/**
 * @author xduo
 * 
 */
public class BeanValidator {

    /**
     * Tests the get/set methods of the specified class.
     */
    public static <T> void validateAccssor(final Class<T> clazz, final String... skipThese) throws IntrospectionException {
        final PropertyDescriptor[] props = Introspector.getBeanInfo(clazz).getPropertyDescriptors();
        for (PropertyDescriptor prop : props) {

            for (String skipThis : skipThese) {
                if (skipThis.equals(prop.getName())) {
                    continue;
                }
            }

            findBooleanIsMethods(clazz, prop);

            final Method getter = prop.getReadMethod();
            final Method setter = prop.getWriteMethod();

            if (getter != null && setter != null) {
                final Class<?> returnType = getter.getReturnType();
                final Class<?>[] params = setter.getParameterTypes();

                if (params.length == 1 && params[0] == returnType) {
                    try {
                        Object value = buildValue(returnType);

                        T bean = clazz.newInstance();

                        setter.invoke(bean, value);

                        Assert.assertEquals(String.format("Failed while testing property %s", prop.getName()), value, getter.invoke(bean));

                    } catch (Exception ex) {
                        ex.printStackTrace();
                        System.err.println(String.format("An exception was thrown while testing the property %s: %s", prop.getName(), ex.toString()));
                    }
                }
            }
        }
    }

    private static Object buildValue(Class<?> clazz) throws InstantiationException, IllegalAccessException, IllegalArgumentException, SecurityException, InvocationTargetException {

        final Constructor<?>[] ctrs = clazz.getConstructors();
        for (Constructor<?> ctr : ctrs) {
            if (ctr.getParameterTypes().length == 0) {
                return ctr.newInstance();
            }
        }

        // Specific rules for common classes
        if (clazz.isArray()) {
            return Array.newInstance(clazz.getComponentType(), 1);
        } else if (List.class.isAssignableFrom(clazz)) {
            return Collections.emptyList();
        } else if (Set.class.isAssignableFrom(clazz)) {
            return Collections.emptySet();
        } else if (Map.class.isAssignableFrom(clazz)) {
            return Collections.emptyMap();
        } else if (clazz == String.class) {
            return "TEST";
        } else if (clazz == boolean.class || clazz == Boolean.class) {
            return true;
        } else if (clazz == short.class || clazz == Short.class) {
            return (short) 1;
        } else if (clazz == int.class || clazz == Integer.class) {
            return 1;
        } else if (clazz == long.class || clazz == Long.class) {
            return 1L;
        } else if (clazz == double.class || clazz == Double.class) {
            return 1.0D;
        } else if (clazz == float.class || clazz == Float.class) {
            return 1.0F;
        } else if (clazz == char.class || clazz == Character.class) {
            return 'T';
        } else if (clazz.isEnum()) {
            return clazz.getEnumConstants()[0];
        } else if (clazz.isInterface()) {
            return Proxy.newProxyInstance(clazz.getClassLoader(), new java.lang.Class[] { clazz }, new java.lang.reflect.InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if (Object.class.getMethod("equals", Object.class).equals(method)) {
                        return proxy == args[0];
                    }
                    if (Object.class.getMethod("hashCode", Object.class).equals(method)) {
                        return Integer.valueOf(System.identityHashCode(proxy));
                    }
                    if (Object.class.getMethod("toString", Object.class).equals(method)) {
                        return "Bean " + getMockedType(proxy);
                    }

                    return null;
                }

            });
        } else {
            System.err.println("Unable to build an instance of class " + clazz.getName() + ", please add some code to the " + BeanValidator.class.getName() + " class to do this.");
            return null;
        }
    }

    public static <T> void findBooleanIsMethods(Class<T> clazz, PropertyDescriptor descriptor) throws IntrospectionException {
        if (descriptor.getReadMethod() == null && descriptor.getPropertyType() == Boolean.class) {
            try {
                PropertyDescriptor pd = new PropertyDescriptor(descriptor.getName(), clazz);
                descriptor.setReadMethod(pd.getReadMethod());
            } catch (IntrospectionException e) {
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, V extends T> Class<T> getMockedType(final V proxy) {
        if (Proxy.isProxyClass(proxy.getClass())) {
            return (Class<T>) proxy.getClass().getInterfaces()[0];
        }
        return (Class<T>) proxy.getClass().getSuperclass();
    }
}
