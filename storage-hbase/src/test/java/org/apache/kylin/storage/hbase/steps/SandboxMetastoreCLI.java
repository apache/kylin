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

package org.apache.kylin.storage.hbase.steps;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;

/**
 * This is a helper class for developer to directly manipulate the metadata store in sandbox
 * This is designed to run in IDE(i.e. not on sandbox hadoop CLI)
 *
 * For production metadata store manipulation refer to bin/metastore.sh in binary package
 * It is desinged to run in hadoop CLI, both in sandbox or in real hadoop environment
 */
public class SandboxMetastoreCLI {

    private static final Log logger = LogFactory.getLog(SandboxMetastoreCLI.class);

    public static void main(String[] args) throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
        if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
            throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.2.4.2-2");
        }

        if (args.length < 2) {
            printUsage();
            return;
        }

        if ("download".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "download", args[1] });
        } else if ("upload".equalsIgnoreCase(args[0])) {
            ResourceTool.main(new String[] { "upload", args[1] });
        } else {
            printUsage();
        }
    }

    private static void printUsage() {
        logger.info("Usage: SandboxMetastoreCLI download toFolder");
        logger.info("Usage: SandboxMetastoreCLI upload   fromFolder");
    }
}
