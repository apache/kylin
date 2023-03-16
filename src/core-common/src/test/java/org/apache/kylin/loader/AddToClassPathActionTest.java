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
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.loader.utils.ClassLoaderUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class AddToClassPathActionTest extends NLocalFileMetadataTestCase {

    private static final String JAR_NAME = "custom_parser.jar";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRun() {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());

        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        String jarAbsPath = new File(jarPath.toString()).toURI().toString();
        HashSet<String> newPaths = Sets.newHashSet(jarAbsPath);

        {
            AddToClassPathAction action1 = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(),
                    newPaths);
            ParserClassLoader parserClassLoader1 = action1.run();
            Assert.assertNotNull(parserClassLoader1);

            AddToClassPathAction action2 = new AddToClassPathAction(parserClassLoader1, newPaths);
            ParserClassLoader parserClassLoader2 = action2.run();
            Assert.assertNotNull(parserClassLoader2);
        }
        {
            // 3 ParserClassLoader
            ParserClassLoader parserClassLoader3 = new ParserClassLoader(
                    newPaths.stream().map(ClassLoaderUtils::urlFromPathString).toArray(URL[]::new));
            try {
                parserClassLoader3.close();
            } catch (IOException e) {
                Assert.fail();
            }
        }

        {
            // 4 ParserClassLoader
            ParserClassLoader parserClassLoader4 = new ParserClassLoader(
                    newPaths.stream().map(ClassLoaderUtils::urlFromPathString).toArray(URL[]::new));
            try {
                parserClassLoader4.close();
                parserClassLoader4.addURL(ClassLoaderUtils.urlFromPathString(jarAbsPath));
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalStateException);
            }
        }

        // 4 throw exception
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new AddToClassPathAction(null, newPaths);
        });
    }
}
