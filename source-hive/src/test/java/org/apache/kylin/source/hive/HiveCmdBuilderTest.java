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

package org.apache.kylin.source.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveCmdBuilderTest {

    @Before
    public void setup() {
        System.setProperty("KYLIN_CONF", LocalFileMetadataTestCase.LOCALMETA_TEST_DATA);
    }

    @After
    public void after() throws Exception {
        System.clearProperty("kylin.hive.client");
        System.clearProperty("kylin.hive.beeline.params");
    }

    @Test
    public void testHiveCLI() {
        System.setProperty("kylin.hive.client", "cli");

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement("USE default;");
        hiveCmdBuilder.addStatement("DROP TABLE test;");
        hiveCmdBuilder.addStatement("SHOW\n TABLES;");

        assertEquals("hive -e \"USE default;\nDROP TABLE test;\nSHOW\n TABLES;\n\"", hiveCmdBuilder.build());
    }

    @Test
    public void testBeeline() throws IOException {
        String lineSeparator = java.security.AccessController.doPrivileged(new sun.security.action.GetPropertyAction("line.separator"));
        System.setProperty("kylin.hive.client", "beeline");
        System.setProperty("kylin.hive.beeline.params", "-u jdbc_url");

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement("USE default;");
        hiveCmdBuilder.addStatement("DROP TABLE test;");
        hiveCmdBuilder.addStatement("SHOW\n TABLES;");

        String cmd = hiveCmdBuilder.build();
        assertTrue(cmd.startsWith("beeline -u jdbc_url -f") && cmd.contains(";rm -f"));

        String hqlFile = cmd.substring(cmd.lastIndexOf("-f ") + 3).trim();
        String hqlStatement = FileUtils.readFileToString(new File(hqlFile), Charset.defaultCharset());
        assertEquals("USE default;" + lineSeparator + "DROP TABLE test;" + lineSeparator + "SHOW\n TABLES;" + lineSeparator, hqlStatement);

        FileUtils.forceDelete(new File(hqlFile));
    }
}