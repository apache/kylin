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

package org.apache.kylin.metadata.project;

import static org.apache.kylin.metadata.project.NProjectLoaderTest.PROJECT_NAME;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@MetadataInfo(overlay = "src/test/resources/ut_meta/project_loader", project = PROJECT_NAME)
class NProjectLoaderTest {

    static final String PROJECT_NAME = "test_cache_loader";

    @RepeatedTest(value = 10)
    void testGetRealizationsByTable() {
        NProjectLoader.removeCache();
        NProjectLoader.updateCache(PROJECT_NAME);
        Set<IRealization> realizations = new NProjectLoader(KylinConfig.getInstanceFromEnv())
                .getRealizationsByTable(PROJECT_NAME, "SSB.P_LINEORDER");
        Assertions.assertEquals(5, realizations.size());
        NProjectLoader.removeCache();
    }

    @Test
    public void testGetEffectiveRewriteMeasures() {
        NProjectLoader.removeCache();
        NProjectLoader.updateCache(PROJECT_NAME);
        List<MeasureDesc> effectiveRewriteMeasures = new NProjectLoader(KylinConfig.getInstanceFromEnv())
                .listEffectiveRewriteMeasures(PROJECT_NAME, "SSB.P_LINEORDER");

        List<MeasureDesc> cacheEffectiveRewriteMeasures = new NProjectLoader(KylinConfig.getInstanceFromEnv())
                .listEffectiveRewriteMeasures(PROJECT_NAME, "SSB.P_LINEORDER");
        Assertions.assertEquals(effectiveRewriteMeasures.size(), cacheEffectiveRewriteMeasures.size());
        NProjectLoader.removeCache();
    }
}
