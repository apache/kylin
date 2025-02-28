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

package org.apache.kylin.loader;

import java.io.File;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ParserClassLoaderStateTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static String JAR_ABS_PATH;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        init();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        ParserClassLoaderState.getInstance(PROJECT).setLoadedJars(Sets.newCopyOnWriteArraySet());
    }

    public void init() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toURI().toString();
    }

    @Test
    public void testRegisterAndUnRegisterJar() {

        HashSet<String> newPaths = Sets.newHashSet(JAR_ABS_PATH);
        ParserClassLoaderState instance = ParserClassLoaderState.getInstance(PROJECT);

        // 1 normal
        instance.registerJars(newPaths);

        // 2 register double jar exception
        try {
            instance.registerJars(newPaths);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        instance.unregisterJar(newPaths);

        // 3 exception
        thrown.expect(IllegalArgumentException.class);
        instance.setLoadedJars(null);
        instance.unregisterJar(Sets.newHashSet("test"));
    }
}
