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

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class KylinVersion {
    private static final String COMMIT_SHA1_v15 = "commit_SHA1";
    private static final String COMMIT_SHA1_v13 = "commit.sha1";

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
    private static final KylinVersion CURRENT_KYLIN_VERSION = new KylinVersion("1.5.4.1");

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

    public static void main(String[] args) {
        System.out.println(getKylinClientInformation());
    }

    public static String getKylinClientInformation() {
        StringBuilder buf = new StringBuilder();

        buf.append("kylin.home: ").append(KylinConfig.getKylinHome() == null ? "UNKNOWN" : new File(KylinConfig.getKylinHome()).getAbsolutePath()).append("\n");
        buf.append("kylin.version:").append(KylinVersion.getCurrentVersion()).append("\n");
        buf.append("commit:").append(getGitCommitInfo()).append("\n");
        buf.append("os.name:").append(System.getProperty("os.name")).append("\n");
        buf.append("os.arch:").append(System.getProperty("os.arch")).append("\n");
        buf.append("os.version:").append(System.getProperty("os.version")).append("\n");
        buf.append("java.version:").append(System.getProperty("java.version")).append("\n");
        buf.append("java.vendor:").append(System.getProperty("java.vendor"));

        return buf.toString();
    }

    public static String getGitCommitInfo() {
        try {
            File commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_v15);
            if (!commitFile.exists()) {
                commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_v13);
            }
            List<String> lines = FileUtils.readLines(commitFile, Charset.defaultCharset());
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                if (!line.startsWith("#")) {
                    sb.append(line).append(";");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }
}