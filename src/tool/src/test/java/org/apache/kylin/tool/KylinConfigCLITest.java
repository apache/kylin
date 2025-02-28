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
package org.apache.kylin.tool;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KylinConfigCLITest extends NLocalFileMetadataTestCase {
    @Test
    public void testGetProperty() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(Files.newOutputStream(f.toPath()), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { "kylin.env" });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("UT", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetEncryptProperty() throws IOException {
        final KylinConfig config = getTestConfig();
        String property = "kylin.influxdb.password";
        config.setProperty(property, "ENC('YeqVr9MakSFbgxEec9sBwg==')");
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(Files.newOutputStream(f.toPath()), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { property, EncryptUtil.DEC_FLAG });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("kylin", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetUnEncryptPropertyWithDECFlag() throws IOException {
        final KylinConfig config = getTestConfig();
        String property = "kylin.influxdb.password";
        config.setProperty(property, "kylin");
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(Files.newOutputStream(f.toPath()), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { property, EncryptUtil.DEC_FLAG });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("kylin", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
}
