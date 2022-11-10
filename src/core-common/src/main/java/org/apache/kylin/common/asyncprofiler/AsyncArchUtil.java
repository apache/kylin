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

package org.apache.kylin.common.asyncprofiler;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class AsyncArchUtil {

    public enum ArchType {
        LINUX_X64(), LINUX_ARM64()
    }

    private static final Map<String, ArchType> ARCH_TO_PROCESSOR = new HashMap<>();

    static {
        addProcessors(ArchType.LINUX_X64, "x86_64", "amd64");
        addProcessors(ArchType.LINUX_ARM64, "aarch64");
    }

    public static ArchType getProcessor() {
        return getProcessor(getSystemProperty("os.arch"));
    }

    /**
     * Returns a {@link ArchType} object with the given value {@link String}. The {@link String} must be
     * like a value returned by the os.arch System Property.
     *
     * @param osArch A {@link String} like a value returned by the os.arch System Property.
     * @return A {@link ArchType} when it exists, else {@code null}.
     */
    public static ArchType getProcessor(final String osArch) {
        return ARCH_TO_PROCESSOR.get(osArch);
    }

    /**
     * <p>
     * Gets a System property, defaulting to {@code null} if the property cannot be read.
     * </p>
     * <p>
     * If a {@code SecurityException} is caught, the return value is {@code null} and a message is written to
     * {@code System.err}.
     * </p>
     *
     * @param property the system property name
     * @return the system property value or {@code null} if a security problem occurs
     */
    private static String getSystemProperty(final String property) {
        try {
            return System.getProperty(property);
        } catch (final SecurityException ex) {
            // we are not allowed to look at this property, the SystemUtils property value will default to null.
            return null;
        }
    }

    private static void addProcessors(ArchType archType, final String... keys) {
        Stream.of(keys).forEach(key -> ARCH_TO_PROCESSOR.put(key, archType));
    }
}