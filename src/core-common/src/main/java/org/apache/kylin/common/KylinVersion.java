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

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class KylinVersion implements Comparable {
    private static final String COMMIT_SHA1_v15 = "commit_SHA1";
    private static final String COMMIT_SHA1_v13 = "commit.sha1";

    public final int major;
    public final int minor;
    public final int revision;
    public final int internal;
    public final boolean isSnapshot;

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
        revision = splits.length > 2 ? Integer.parseInt(splits[2]) : 0;
        internal = splits.length > 3 ? Integer.parseInt(splits[3]) : 0;
    }

    @Override
    public String toString() {
        return "" + major + "." + minor + "." + revision + "." + internal + (isSnapshot ? "-SNAPSHOT" : "");
    }

    @Override
    public int compareTo(Object o) {
        KylinVersion v = (KylinVersion) o;
        int comp;

        comp = this.major - v.major;
        if (comp != 0)
            return comp;

        comp = this.minor - v.minor;
        if (comp != 0)
            return comp;

        comp = this.revision - v.revision;
        if (comp != 0)
            return comp;

        comp = this.internal - v.internal;
        if (comp != 0)
            return comp;

        return (this.isSnapshot ? 0 : 1) - (v.isSnapshot ? 0 : 1);
    }

    /**
     * Require MANUAL updating kylin version per ANY upgrading.
     */
    private static final KylinVersion CURRENT_KYLIN_VERSION = new KylinVersion("4.0.0");

    private static final KylinVersion VERSION_200 = new KylinVersion("2.0.0");

    private static final Set<KylinVersion> SIGNATURE_INCOMPATIBLE_REVISIONS = new HashSet<KylinVersion>();

    /**
     * 1.5.1 is actually compatible with 1.5.0's cube. However the "calculate signature" method in 1.5.1 code base somehow
     * gives different signature values for 1.5.0 cubes. To prevent from users having to take care of this mess, as people
     * usually won't expect to take lots of efforts for small upgrade (from 1.5.0 to 1.5.1), a special list of
     * SIGNATURE_INCOMPATIBLE_REVISIONS is introduced to silently take care of such legacy cubes.
     *
     * We should NEVER add new stuff to SIGNATURE_INCOMPATIBLE_REVISIONS. "calculate signature" should always return consistent values
     * to compatible versions. If it's impossible to maintain consistent signatures between upgrade, we should increase the minor version,
     * e.g. it's better to skip 1.5.1 and use 1.6.0 as the next release version to 1.5.0, or even to use 2.0.0, as people tends to accept
     * doing more (e.g. Having to use sth like a metastore upgrade tool when upgrading Kylin)
     */
    static {
        SIGNATURE_INCOMPATIBLE_REVISIONS.add(new KylinVersion("1.5.1"));
        SIGNATURE_INCOMPATIBLE_REVISIONS.add(new KylinVersion("1.6.1"));
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

    public static boolean isBefore200(String ver) {
        return new KylinVersion(ver).compareTo(VERSION_200) < 0;
    }

    public static int compare(String v1, String v2) {
        return new KylinVersion(v1).compareTo(new KylinVersion(v2));
    }

    public static void main(String[] args) {
        System.out.println(getKylinClientInformation());
    }

    public boolean isCompatibleWith(KylinVersion v) {
        KylinVersion current = CURRENT_KYLIN_VERSION;
        return current.major == v.major && current.minor == v.minor;
    }

    public boolean isSignatureCompatibleWith(final KylinVersion v) {
        if (!isCompatibleWith(v)) {
            return false;
        }

        if (v.isSnapshot || isSnapshot) {
            return false;//for snapshot versions things are undetermined
        }

        boolean signatureIncompatible = Iterables.any(Iterables.filter(

                SIGNATURE_INCOMPATIBLE_REVISIONS,
                input -> input != null && v.major == input.major && v.minor == input.minor),
                input -> input != null && input.revision > v.revision);

        return !signatureIncompatible;
    }

    public static String getKylinClientInformation() {
        StringBuilder buf = new StringBuilder();

        buf.append("kylin.home: ").append(
                KylinConfig.getKylinHome() == null ? "UNKNOWN" : new File(KylinConfig.getKylinHome()).getAbsolutePath())
                .append("\n");
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
