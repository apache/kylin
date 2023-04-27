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

package org.apache.kylin.tool.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.val;
import lombok.var;

@MetadataInfo
class HiveClientJarToolWithoutSparkHiveDirTest extends HiveClientJarToolTestBase {
    @BeforeEach
    public void before() throws IOException {
        super.before();
    }

    @AfterEach
    public void after() throws IOException {
        super.after();
        val sparkHome = KylinConfig.getSparkHome();
        val sparkPath = Paths.get(sparkHome);
        val hive122 = sparkHome + File.separator + "hive_1_2_2";
        val hive122Path = Paths.get(hive122);
        Files.deleteIfExists(hive122Path);
        Files.deleteIfExists(sparkPath);
    }

    @Test
    void getKylinSparkHiveJarsPath() throws IOException {
        val sparkHome = KylinConfig.getSparkHome();
        val sparkPath = Paths.get(sparkHome);
        val hive122 = sparkHome + File.separator + "hive_1_2_2";
        val hive122Path = Paths.get(hive122);
        try {
            var kylinSparkHiveJarsPath = uploadHiveJarsTool.getKylinSparkHiveJarsPath();
            assertTrue(StringUtils.isBlank(kylinSparkHiveJarsPath));
            Files.createDirectory(sparkPath);
            Files.createFile(hive122Path);
            kylinSparkHiveJarsPath = uploadHiveJarsTool.getKylinSparkHiveJarsPath();
            assertTrue(StringUtils.isBlank(kylinSparkHiveJarsPath));

            Files.deleteIfExists(hive122Path);
            Files.createDirectory(hive122Path);
            kylinSparkHiveJarsPath = uploadHiveJarsTool.getKylinSparkHiveJarsPath();
            assertEquals(new File(hive122).getCanonicalPath(), kylinSparkHiveJarsPath);
        } finally {
            Files.deleteIfExists(hive122Path);
            Files.deleteIfExists(sparkPath);
        }
    }

    @Test
    void testExecute() throws IOException {
        testExecute(true, true, "${KYLIN_HOME}/spark/hive_1_2_2 needs to be an existing directory");
    }

    @Test
    void testExecuteWithHive122File() throws IOException {
        val sparkHome = KylinConfig.getSparkHome();
        val sparkPath = Paths.get(sparkHome);
        val hive122 = sparkHome + File.separator + "hive_1_2_2";
        val hive122Path = Paths.get(hive122);
        Files.createDirectories(sparkPath);
        Files.createFile(hive122Path);
        testExecute(true, true, "${KYLIN_HOME}/spark/hive_1_2_2 needs to be an existing directory");
    }
}
