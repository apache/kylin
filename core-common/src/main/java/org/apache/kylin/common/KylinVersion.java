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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class KylinVersion {

    public int major;
    public int minor;
    public int revision;
    public boolean isSnapshot;

    public KylinVersion(String version) {

        Preconditions.checkNotNull(version);

        int index = version.indexOf("-");//index of "-SNAPSHOT"
        String[] splits;
        if (index == -1) {
            splits = version.split("\\.");
            isSnapshot = false;
        } else {
            splits = version.substring(0, index).split("\\.");
            isSnapshot = true;
        }

        major = Integer.parseInt(splits[0]);
        minor = Integer.parseInt(splits[1]);
        revision = Integer.parseInt(splits[2]);
    }

    @Override
    public String toString() {
        return "" + major + "." + minor + "." + revision;
    }

    /**
     * Require MANUAL updating kylin version per ANY upgrading.
     */
    private static final KylinVersion CURRENT_KYLIN_VERSION = new KylinVersion("1.5.1");

    private static final Set<KylinVersion> SIGNATURE_INCOMPATIBLE_REVISIONS = new HashSet<KylinVersion>();

    static {
        SIGNATURE_INCOMPATIBLE_REVISIONS.add(new KylinVersion("1.5.1"));
    }

    /**
     * Get current Kylin version
     * <p/>
     * Currently the implementation is reading directly from constant variable
     *
     * @return current kylin version in String
     */
    public static KylinVersion getCurrentVersion() {
        return CURRENT_KYLIN_VERSION;
    }

    public boolean isCompatibleWith(KylinVersion v) {
        KylinVersion current = CURRENT_KYLIN_VERSION;
        if (current.major != v.major || current.minor != v.minor) {
            return false;
        } else {
            return true;
        }
    }

    public boolean isSignatureCompatibleWith(final KylinVersion v) {
        if (!isCompatibleWith(v)) {
            return false;
        }

        if (v.isSnapshot || isSnapshot) {
            return false;//for snapshot versions things are undetermined
        }

        boolean signatureIncompatible = Iterables.any(Iterables.filter(

                SIGNATURE_INCOMPATIBLE_REVISIONS, new Predicate<KylinVersion>() {
                    @Override
                    public boolean apply(@Nullable KylinVersion input) {
                        return v.major == input.major && v.minor == input.minor;
                    }
                }), new Predicate<KylinVersion>() {
                    @Override
                    public boolean apply(@Nullable KylinVersion input) {
                        return input.revision > v.revision;
                    }
                });

        return !signatureIncompatible;
    }
}