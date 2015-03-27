/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.job.streaming.StreamingBootstrap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 * Created by qianzhou on 3/6/15.
 */
public class IIStreamBuilderTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.0.0-2041"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
        kylinConfig = KylinConfig.getInstanceFromEnv();
        DeployUtil.initCliWorkDir();
        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        StreamingBootstrap.getInstance(kylinConfig).startStreaming("eagle", 0);
    }
}
