/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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

    public static final String N_KAP_META_TEST_DATA = "../examples/test_case_data/localmeta_n";
    public static final String N_SPARK_PROJECT_KAP_META_TEST_DATA = "/Users/rupeng.wang/Kyligence/Developments/kylin/kylin-parquet/examples/test_case_data/localmeta_n";
    public static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    private static final Logger logger = LoggerFactory.getLogger(TempMetadataBuilder.class);

    public static String prepareNLocalTempMetadata() {
        return prepareNLocalTempMetadata(false);
    }

    public static String prepareNLocalTempMetadata(boolean debug) {
        // for spark-project
        if (!new File(N_KAP_META_TEST_DATA).exists()) {
            return new TempMetadataBuilder(debug, N_SPARK_PROJECT_KAP_META_TEST_DATA).build();
        }
        return new TempMetadataBuilder(debug, N_KAP_META_TEST_DATA).build();
    }

    public static String prepareNLocalTempMetadata(boolean debug, String... overlay) {
        String[] nOverlay = new String[overlay.length + 1];
        nOverlay[0] = N_KAP_META_TEST_DATA;
        System.arraycopy(overlay, 0, nOverlay, 1, overlay.length);
        // for spark-project
        if (!new File(nOverlay[0]).exists()) {
            nOverlay[0] = "../" + nOverlay[0];
        }
        return new TempMetadataBuilder(debug, nOverlay).build();
    }

    // ============================================================================

    private String[] metaSrcs = null;
    private boolean debug = false;

    private TempMetadataBuilder(boolean debug, String... metaSrcs) {
        this.metaSrcs = metaSrcs;
        this.debug = debug;
    }

    public String build() {

        if ("true".equals(System.getProperty("skipMetaPrep"))) {
            return TEMP_TEST_METADATA;
        }

        try {
            if (debug) {
                logger.info("Preparing local temp metadata");
                for (String metaSrc : metaSrcs) {
                    logger.info("Found one META_TEST_SRC: {}", new File(metaSrc).getCanonicalPath());
                }
                logger.info("TEMP_TEST_METADATA={}", new File(TEMP_TEST_METADATA).getCanonicalPath());
            }

            FileUtils.deleteQuietly(new File(TEMP_TEST_METADATA));

            // KAP files will overwrite Kylin files
            for (String metaSrc : metaSrcs) {
                FileUtils.copyDirectory(new File(metaSrc), new File(TEMP_TEST_METADATA));
            }

            appendKylinProperties(TEMP_TEST_METADATA);
            //overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, grabDefaultEngineTypes(TEMP_TEST_METADATA), null);

            if (debug) {
                File copy = new File(TEMP_TEST_METADATA + ".debug");
                FileUtils.deleteDirectory(copy);
                FileUtils.copyDirectory(new File(TEMP_TEST_METADATA), copy);
                logger.info("Make copy for debug: {}", copy.getCanonicalPath());
            }

            return TEMP_TEST_METADATA;
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
