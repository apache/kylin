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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KylinConfigCheckCLITest extends NLocalFileMetadataTestCase {
    @Test
    public void testCorrectConfig() throws IOException {
        final File kylinConfDir = KylinConfig.getKylinConfDir();
        File kylin_properties_override = new File(kylinConfDir, "kylin.properties.override");
        IOUtils.copy(new ByteArrayInputStream("kylin.kerberos.platform=FI".getBytes(Charset.defaultCharset())),
                new FileOutputStream(kylin_properties_override));

        PrintStream o = System.out;
        File f = File.createTempFile("check", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);

        KylinConfigCheckCLI.execute();

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();

        assertEquals("", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        FileUtils.forceDelete(kylin_properties_override);
        System.setOut(o);
    }

    @Test
    public void testHasErrorConfig() throws IOException {
        final File kylinConfDir = KylinConfig.getKylinConfDir();
        File kylin_properties_override = new File(kylinConfDir, "kylin.properties.override");
        IOUtils.copy(new ByteArrayInputStream("n.kerberos.platform=FI".getBytes(Charset.defaultCharset())),
                new FileOutputStream(kylin_properties_override));

        PrintStream o = System.out;
        File f = File.createTempFile("check", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);

        KylinConfigCheckCLI.execute();

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();

        assertEquals("n.kerberos.platform", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        FileUtils.forceDelete(kylin_properties_override);
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
