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

package org.apache.kylin.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.Before;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SemiAutoTestBase extends SuggestTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        dataflowManager.listAllDataflows().stream().map(RootPersistentEntity::getId)
                .forEach(dataflowManager::dropDataflow);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexPlanManager.listAllIndexPlans().forEach(indexPlanManager::dropIndexPlan);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModelIds().forEach(modelManager::dropModel);
    }

    @Override
    protected AbstractContext proposeWithSmartMaster(String project, List<TestScenario> testScenarios)
            throws IOException {
        List<String> sqlList = collectQueries(testScenarios);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));
        String[] sqls = sqlList.toArray(new String[0]);
        return AccelerationUtil.runModelReuseContext(getTestConfig(), project, sqls);
    }

    @Override
    protected Map<String, ExecAndCompExt.CompareEntity> collectCompareEntity(AbstractContext context) {
        Map<String, ExecAndCompExt.CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new ExecAndCompExt.CompareEntity());
            final ExecAndCompExt.CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
            entity.setLevel(RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY);
        });
        return map;
    }

    @Override
    protected void buildAndCompare(TestScenario... testScenarios) throws Exception {
        try {
            buildAllModels(kylinConfig, getProject());
            compare(testScenarios);
        } finally {
            FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        }
    }

    protected void compare(TestScenario... testScenarios) {
        Arrays.stream(testScenarios).forEach(this::compare);
    }

    private void compare(TestScenario scenario) {
        NLocalWithSparkSessionTestBase.populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        if (scenario.isLimit()) {
            ExecAndCompExt.execLimitAndValidateNew(scenario, getProject(), JoinType.DEFAULT.name(), null);
        } else if (scenario.isDynamicSql()) {
            ExecAndCompExt.execAndCompareDynamic(scenario, getProject(), scenario.getCompareLevel(),
                    scenario.getJoinType().name(), null);
        } else {
            ExecAndCompExt.execAndCompare(scenario, getProject(), scenario.getCompareLevel(),
                    scenario.getJoinType().name());
        }
    }
}
