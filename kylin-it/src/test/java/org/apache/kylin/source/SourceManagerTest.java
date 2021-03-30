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

package org.apache.kylin.source;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ISourceAware;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SourceManagerTest extends LocalFileMetadataTestCase {
    @BeforeClass
    public static void beforeClass() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testGetSource() {
        final KylinConfig config = getTestConfig();
        SourceManager sourceManager = SourceManager.getInstance(config);
        ISource source = sourceManager.getCachedSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return config.getDefaultSource();
            }

            @Override
            public KylinConfig getConfig() {
                return config;
            }
        });

        Assert.assertEquals(config.getSourceEngines().get(config.getDefaultSource()), source.getClass().getName());
        Assert.assertEquals(source, SourceManager.getDefaultSource());
        Assert.assertEquals(source, SourceManager.getInstance(getTestConfig()).getProjectSource(null));
    }

}
