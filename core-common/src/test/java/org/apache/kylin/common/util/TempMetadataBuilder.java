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

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TempMetadataBuilder {

    /**
     * Provided metadata for Parquet Storage
     */
    public static final String N_SPARK_PROJECT_KYLIN_META_TEST_DATA = "../../examples/test_case_data/parquet_test";
    public static final String N_SPARK_PROJECT_KYLIN_META_TEST_DATA_2 = "../examples/test_case_data/parquet_test";

    /**
     * Temporary metadata dir
     */
    public static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    private static final Logger logger = LoggerFactory.getLogger(TempMetadataBuilder.class);

    /**
     * @see TempMetadataBuilder#TEMP_TEST_METADATA
     */
    public static String prepareNLocalTempMetadata() {
        return prepareNLocalTempMetadata(false);
    }

    public static String prepareNLocalTempMetadata(boolean debug) {
        if (new File(N_SPARK_PROJECT_KYLIN_META_TEST_DATA).exists()) {
            return new TempMetadataBuilder(debug, "../" + TEMP_TEST_METADATA, N_SPARK_PROJECT_KYLIN_META_TEST_DATA).build();
        }
        return new TempMetadataBuilder(debug, TEMP_TEST_METADATA, N_SPARK_PROJECT_KYLIN_META_TEST_DATA_2).build();
    }

    public static String prepareNLocalTempMetadata(boolean debug, String overlay) {
        if (new File(overlay).exists()) {
            return new TempMetadataBuilder(debug, "../" + TEMP_TEST_METADATA, overlay).build();
        }
        return new TempMetadataBuilder(debug, TEMP_TEST_METADATA, overlay).build();
    }

    // ============================================================================

    private String[] metaSrcs = null;
    private boolean debug = false;
    private String dst;

    private TempMetadataBuilder(boolean debug, String dst, String... metaSrcs) {
        this.metaSrcs = metaSrcs;
        this.debug = debug;
        this.dst = dst;
    }

    private String build() {
        logger.debug("Prepare temp metadata for ut/it .");
        if ("true".equals(System.getProperty("skipMetaPrep"))) {
            return dst;
        }

        try {
            if (debug) {
                logger.info("Preparing local temp metadata");
                for (String metaSrc : metaSrcs) {
                    logger.info("Found one META_TEST_SRC: {}", new File(metaSrc).getCanonicalPath());
                }
                logger.info("TEMP_TEST_METADATA={}", new File(TEMP_TEST_METADATA).getCanonicalPath());
            }

            FileUtils.deleteQuietly(new File(dst));

            for (String metaSrc : metaSrcs) {
                FileUtils.copyDirectory(new File(metaSrc), new File(dst));
            }

            appendKylinProperties(dst);
            //overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, grabDefaultEngineTypes(TEMP_TEST_METADATA), null);

            if (debug) {
                File copy = new File(dst + ".debug");
                FileUtils.deleteDirectory(copy);
                FileUtils.copyDirectory(new File(dst), copy);
                logger.info("Make copy for debug: {}", copy.getCanonicalPath());
            }

            return dst;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendKylinProperties(String tempMetadataDir) throws IOException {
        File propsFile = new File(tempMetadataDir, "kylin.properties");

        // append kylin.properties
        File appendFile = new File(tempMetadataDir, "kylin.properties.append");
        if (appendFile.exists()) {
            if (debug) {
                logger.info("Appending kylin.properties from {}", appendFile.getCanonicalPath());
            }

            String appendStr = FileUtils.readFileToString(appendFile, Charsets.UTF_8);
            FileUtils.writeStringToFile(propsFile, appendStr, Charsets.UTF_8, true);
            FileUtils.deleteQuietly(appendFile);
        }
    }

    private void overrideEngineTypeAndStorageType(String tempMetadataDir, Pair<Integer, Integer> typePair,
            List<String> includeFiles) throws IOException {
        int engineType = typePair.getFirst();
        int storageType = typePair.getSecond();

        if (debug) {
            logger.info("Override engine type to be {}", engineType);
            logger.info("Override storage type to be {}", storageType);
        }

        // re-write cube_desc/*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        File[] cubeDescFiles = cubeDescDir.listFiles();
        if (cubeDescFiles == null)
            return;

        for (File f : cubeDescFiles) {
            if (includeFiles != null && !includeFiles.contains(f.getName())) {
                continue;
            }
            if (debug) {
                logger.info("Process override {}", f.getCanonicalPath());
            }
            List<String> lines = FileUtils.readLines(f, Charsets.UTF_8);
            for (int i = 0, n = lines.size(); i < n; i++) {
                String l = lines.get(i);
                if (l.contains("\"engine_type\"")) {
                    lines.set(i, "  \"engine_type\" : " + engineType + ",");
                }
                if (l.contains("\"storage_type\"")) {
                    lines.set(i, "  \"storage_type\" : " + storageType + ",");
                }
            }
            FileUtils.writeLines(f, "UTF-8", lines);
        }
    }

    private Pair<Integer, Integer> grabDefaultEngineTypes(String tempMetadataDir) throws IOException {
        int engineType = -1;
        int storageType = -1;

        List<String> lines = FileUtils.readLines(new File(tempMetadataDir, "kylin.properties"), Charsets.UTF_8);
        for (String l : lines) {
            if (l.startsWith("kylin.engine.default")) {
                engineType = Integer.parseInt(l.substring(l.lastIndexOf('=') + 1).trim());
            }
            if (l.startsWith("kylin.storage.default")) {
                storageType = Integer.parseInt(l.substring(l.lastIndexOf('=') + 1).trim());
            }
        }

        if (debug) {
            logger.info("Grap from kylin.properties, engine type is {}", engineType);
            logger.info("Grap from kylin.properties, storage type is {}", storageType);
        }

        String tmp = System.getProperty("kylin.engine");
        if (!StringUtils.isBlank(tmp)) {
            engineType = Integer.parseInt(tmp.trim());
            if (debug) {
                logger.info("By system property, engine type is {}", engineType);
            }
        }
        tmp = System.getProperty("kylin.storage");
        if (!StringUtils.isBlank(tmp)) {
            storageType = Integer.parseInt(tmp.trim());
            if (debug) {
                logger.info("By system property, storage type is {}", storageType);
            }
        }

        if (engineType < 0 || storageType < 0)
            throw new IllegalStateException();

        return Pair.newPair(engineType, storageType);
    }

}
