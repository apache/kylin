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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KylinConfigCLITest extends LocalFileMetadataTestCase {
    @Test
    public void testGetProperty() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        System.setOut(new PrintStream(new FileOutputStream(f)));
        KylinConfigCLI.main(new String[] { "kylin.storage.url" });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("hbase", val);

        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetPrefix() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        System.setOut(new PrintStream(new FileOutputStream(f)));
        KylinConfigCLI.main(new String[] { "kylin.cube.engine." });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("2=org.apache.kylin.engine.mr.MRBatchCubingEngine2\n0=org.apache.kylin.engine.mr.MRBatchCubingEngine", val);

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
