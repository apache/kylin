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

package org.apache.kylin.job.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import lombok.val;

@MetadataInfo
class ExecutableContextTest {

    @Test
    void getJobThreads() {
        val job = new DefaultExecutable();
        val context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(),
                KylinConfig.getInstanceFromEnv(), 0);
        context.addRunningJob(job);

        assertNotNull(context.getRunningJobThread(job));

        context.removeRunningJob(job);
        assertNull(context.getRunningJobThread(job));
    }

    @Test
    void testGetFrozenJob() {
        String jobId = "f6384d3e-d46d-5cea-b2d9-28510a2191f3-50b0f62d-e9c1-810b-e499-95aa549c701c";
        val context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(),
                KylinConfig.getInstanceFromEnv(), 0);
        context.addFrozenJob(jobId);
        assertTrue(context.isFrozenJob(jobId));

        context.removeFrozenJob(jobId);
        assertFalse(context.isFrozenJob(jobId));
    }
}