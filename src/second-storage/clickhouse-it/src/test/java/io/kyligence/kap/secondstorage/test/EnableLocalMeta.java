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
package io.kyligence.kap.secondstorage.test;

import java.io.File;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

public class EnableLocalMeta extends ExternalResource implements KapTest {

    private final String[] overlay;
    protected final String project;
    private final NLocalFileMetadataTestCase internalMeta = new NLocalFileMetadataTestCase();

    public EnableLocalMeta(String project, String... extraMeta) {
        this.project = project;
        this.overlay = extraMeta;
    }

    public KylinConfig getTestConfig() {
        return NLocalFileMetadataTestCase.getTestConfig();
    }

    @Override
    protected void before() throws Throwable {
        internalMeta.createTestMetadata(overlay);
        final File tempMetadataDirectory = NLocalFileMetadataTestCase.getTempMetadataDirectory();
        Assert.assertNotNull(tempMetadataDirectory);
        Assert.assertTrue(tempMetadataDirectory.exists());
        final File testProjectMetaDir = new File(tempMetadataDirectory.getPath() + "/metadata/" + project);
        final String message = String.format(Locale.ROOT, "%s's meta (%s) doesn't exist, please check!", project,
                testProjectMetaDir.getCanonicalPath());
        Assert.assertTrue(message, testProjectMetaDir.exists());
    }

    @Override
    protected void after() {
        internalMeta.cleanupTestMetadata();
        internalMeta.restoreSystemProps();
    }

    @Override
    public void overwriteSystemProp(String key, String value) {
        internalMeta.overwriteSystemProp(key, value);
    }
}
