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

package org.apache.kylin.storage.hbase.lookup;

import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class UpdateSnapshotCacheForQueryServersStepTest extends LocalFileMetadataTestCase {
    private KylinConfig kylinConfig;
    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws ExecuteException {
        UpdateSnapshotCacheForQueryServersStep step = new UpdateSnapshotCacheForQueryServersStep();
        ExecuteResult result = step.doWork(new DefaultContext(Maps.<String, Executable>newConcurrentMap(), kylinConfig));
        System.out.println(result.output());
        assertTrue(result.succeed());
    }
}
