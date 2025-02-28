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

package org.apache.kylin.job.common;

import static org.apache.kylin.job.constant.ExecutableConstants.COLUMNAR_SHUFFLE_MANAGER;
import static org.apache.kylin.job.constant.ExecutableConstants.GLUTEN_PLUGIN;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_PLUGINS;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_SHUFFLE_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.model.JobParam;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class ExecutableUtilTest {

    @Test
    public void testComputeJobBucket_throwsException() {
        JobParam jobParam = mock(JobParam.class);
        when(jobParam.isMultiPartitionJob()).thenReturn(true);
        when(jobParam.getTargetPartitions()).thenReturn(Collections.emptySet());
        try {
            ExecutableUtil.computeJobBucket(jobParam);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(
                    "KE-010032201: Can't add the job, as the subpartition value is empty. Please check and try again.",
                    e.toString());
        }
    }

    @Test
    public void removeGultenParams() {
        val requestMap = Maps.<String, String> newHashMap();
        requestMap.put(SPARK_PLUGINS, GLUTEN_PLUGIN);
        var params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(1, params.size());
        assertEquals("", params.get(SPARK_PLUGINS));

        requestMap.put(SPARK_PLUGINS,
                GLUTEN_PLUGIN + ",org.apache.gluten.GlutenPlugin,org.apache.spark.kyuubi.KyuubiPlugin");
        params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(1, params.size());
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", params.get(SPARK_PLUGINS));

        requestMap.put("spark.gluten.enable", "true");
        params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(1, params.size());
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", params.get(SPARK_PLUGINS));

        requestMap.put(SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(2, params.size());
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", params.get(SPARK_PLUGINS));
        assertEquals("org.apache.spark.shuffle.sort.SortShuffleManager", params.get(SPARK_SHUFFLE_MANAGER));

        requestMap.put(SPARK_SHUFFLE_MANAGER, "sort");
        params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(2, params.size());
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", params.get(SPARK_PLUGINS));
        assertEquals("sort", params.get(SPARK_SHUFFLE_MANAGER));

        requestMap.put(SPARK_SHUFFLE_MANAGER, COLUMNAR_SHUFFLE_MANAGER);
        params = ExecutableUtil.removeGultenParams(requestMap);
        assertEquals(2, params.size());
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", params.get(SPARK_PLUGINS));
        assertEquals("sort", params.get(SPARK_SHUFFLE_MANAGER));
    }
}
