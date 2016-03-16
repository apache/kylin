/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.common;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class KylinVersion {

    static class Version {
        public int major;
        public int minor;
        public int revision;

        public Version(String version) {
            String[] splits = version.split("\\.");
            major = Integer.parseInt(splits[0]);
            minor = Integer.parseInt(splits[1]);
            revision = Integer.parseInt(splits[2]);
        }
    }

    /**
     * Require MANUAL updating kylin version per ANY upgrading.
     */
    private static final String CURRENT_KYLIN_VERSION = "1.5.1";

    private static final Set<String> SIGNATURE_INCOMPATIBLE_REVISIONS = new HashSet<String>();

    static {
        SIGNATURE_INCOMPATIBLE_REVISIONS.add("1.5.1");
    }

    /**
     * Get current Kylin version
     * <p/>
     * Currently the implementation is reading directly from constant variable
     *
     * @return current kylin version in String
     */
    public static String getCurrentVersion() {
        return CURRENT_KYLIN_VERSION;
    }

    public static boolean isCompatibleWith(String version) {
        Version v = new Version(version);
        Version current = new Version(CURRENT_KYLIN_VERSION);
        if (current.major != v.major || current.minor != v.minor) {
            return false;
        } else {
            return true;
        }
    }

    public static boolean isSignatureCompatibleWith(String version) {
        if (!isCompatibleWith(version)) {
            return false;
        }
        final Version v = new Version(version);
        boolean signatureIncompatible = Iterables.any(Iterables.filter(

                Iterables.transform(SIGNATURE_INCOMPATIBLE_REVISIONS, new Function<String, Version>() {
                    @Nullable
                    @Override
                    public Version apply(@Nullable String input) {
                        return new Version(input);
                    }
                }), new Predicate<Version>() {
                    @Override
                    public boolean apply(@Nullable Version input) {
                        return v.major == input.major && v.minor == input.minor;
                    }
                }), new Predicate<Version>() {
                    @Override
                    public boolean apply(@Nullable Version input) {
                        return input.revision > v.revision;
                    }
                });

        return !signatureIncompatible;
    }
}