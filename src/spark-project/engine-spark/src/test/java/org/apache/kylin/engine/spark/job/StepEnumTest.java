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

package org.apache.kylin.engine.spark.job;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo
class StepEnumTest {

    @Test
    void testUpdateMetadataCondition() {
        val config = KylinConfig.getInstanceFromEnv();
        val job1 = new DefaultExecutable();
        try {
            StepEnum.UPDATE_METADATA.create(job1, config);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
        val job2 = new DefaultExecutableOnModel();
        job2.setProject("default");
        job2.setJobType(JobTypeEnum.INDEX_BUILD);
        StepEnum.UPDATE_METADATA.create(job2, config);
    }
}
