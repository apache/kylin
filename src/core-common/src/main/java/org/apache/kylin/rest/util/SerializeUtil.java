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

package org.apache.kylin.rest.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.List;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SerializeUtil {

    /**
     * Packages whose classes are allowed to appear in a serialized cache value. Any other class is
     * rejected while the stream is being resolved, before it can be instantiated. This blocks the
     * third-party gadget chains (commons-collections, the JDK internal XML/management classes, and
     * so on) that a poisoned cache entry would otherwise rely on.
     */
    private static final List<String> ALLOWED_CLASS_PREFIXES = Arrays.asList( //
            "org.apache.kylin.", //
            "java.lang.", //
            "java.util.", //
            "java.math.", //
            "java.time.");

    public static byte[] serialize(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException("serialize failed", e);
        }
    }

    public static Object deserialize(byte[] bytes) {
        try (ObjectInputStream ois = new FilteringObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        } catch (Exception e) {
            throw new IllegalStateException("deserialize failed", e);
        }
    }

    static boolean isAllowedClassName(String className) {
        String component = className;
        boolean array = false;
        while (component.startsWith("[")) {
            component = component.substring(1);
            array = true;
        }
        if (array) {
            if (component.length() == 1 && "BCDFIJSZ".indexOf(component.charAt(0)) >= 0) {
                return true;
            }
            if (component.startsWith("L") && component.endsWith(";")) {
                component = component.substring(1, component.length() - 1);
            }
        }
        for (String prefix : ALLOWED_CLASS_PREFIXES) {
            if (component.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    static final class FilteringObjectInputStream extends ObjectInputStream {

        FilteringObjectInputStream(InputStream inputStream) throws IOException {
            super(inputStream);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String className = desc.getName();
            if (!isAllowedClassName(className)) {
                throw new InvalidClassException(className, "class is not allowed for Kylin cache deserialization");
            }
            return super.resolveClass(desc);
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            for (String interfaceName : interfaces) {
                if (!isAllowedClassName(interfaceName)) {
                    throw new InvalidClassException(interfaceName,
                            "proxy interface is not allowed for Kylin cache deserialization");
                }
            }
            return super.resolveProxyClass(interfaces);
        }
    }
}
