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

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;

@RequiredArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class TempMetadataBuilder {

    // TODO Enterprise should have diff implementations
    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String SPARK_PROJECT_KAP_META_TEST_DATA = "../../examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_data/"
            + ProcessUtils.getCurrentId(System.currentTimeMillis() + "_"
            + UUID.randomUUID().toString());

    public static String prepareLocalTempMetadata() {
        return prepareLocalTempMetadata(Lists.newArrayList());
    }

    public static String prepareLocalTempMetadata(List<String> overlay) {
        return createBuilder(overlay).build();
    }

    public static TempMetadataBuilder createBuilder(List<String> overlay) {
        overlay.add(0, KAP_META_TEST_DATA);
        if (!new File(overlay.get(0)).exists()) {
            overlay.set(0, "../" + overlay.get(0));
        }
        return new TempMetadataBuilder(overlay);
    }

    // ============================================================================
    private final List<String> metaSrcs;
    private String project;
    private boolean onlyProps = false;

    public String build() {
        try {
            String tempTestMetadataDir = TEMP_TEST_METADATA;
            FileUtils.deleteQuietly(new File(tempTestMetadataDir));

            for (String metaSrc : metaSrcs) {
                if (onlyProps) {
                    FileUtils.copyFile(new File(metaSrc, "kylin.properties"),
                            new File(tempTestMetadataDir, "kylin.properties"));
                } else {
                    FileUtils.copyDirectory(new File(metaSrc), new File(tempTestMetadataDir), pathname -> {
                        if (pathname.isDirectory()) {
                            return true;
                        }
                        try {
                            val name = pathname.getCanonicalPath();
                            return project == null || name.contains(project) || name.endsWith(".properties");
                        } catch (IOException ignore) {
                            // ignore it
                        }
                        return false;
                    }, true);
                }
            }

            appendKylinProperties(tempTestMetadataDir);
            return tempTestMetadataDir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendKylinProperties(String tempMetadataDir) throws IOException {
        File propsFile = new File(tempMetadataDir, "kylin.properties");

        // append kylin.properties
        File appendFile = new File(tempMetadataDir, "kylin.properties.append");
        if (appendFile.exists()) {
            String appendStr = FileUtils.readFileToString(appendFile, Charsets.UTF_8);
            FileUtils.writeStringToFile(propsFile, appendStr, Charsets.UTF_8, true);
            FileUtils.deleteQuietly(appendFile);
        }
    }

}
