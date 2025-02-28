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

package org.apache.kylin.rec.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.SqlValidateContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryRunnerBuilder {

    private final KylinConfig kylinConfig;
    private final String[] sqls;
    private final String project;
    private List<NDataModel> models = Lists.newArrayList();

    public QueryRunnerBuilder(String project, KylinConfig kylinConfig, String[] sqls) {
        this.kylinConfig = kylinConfig;
        this.sqls = sqls;
        this.project = project;
    }

    public QueryRunnerBuilder of(List<NDataModel> models) {
        this.models = models;
        return this;
    }

    public LocalQueryRunner build() {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        prepareResources(dumpResources, mockupResources, models);
        return new LocalQueryRunner(kylinConfig, project, sqls, dumpResources, mockupResources);
    }

    private void prepareResources(Set<String> dumpResources, Map<String, RootPersistentEntity> mockupResources,
            List<NDataModel> dataModels) {

        ProjectInstance dumpProj = new ProjectInstance();
        dumpProj.setName(project);
        dumpProj.setDefaultDatabase(NProjectManager.getInstance(kylinConfig).getDefaultDatabase(project));
        dumpProj.init(kylinConfig, true);
        mockupResources.put(dumpProj.getResourcePath(), dumpProj);

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(kylinConfig, project);
        AbstractContext context = new SqlValidateContext(kylinConfig, project, sqls);
        Set<String> relatedTables = context.getRelatedTables();
        relatedTables.stream().map(tableManager::getTableDesc).forEach(tableDesc -> {
            dumpResources.add(tableDesc.getResourcePath());
            TableExtDesc tableExt = tableManager.getTableExtIfExists(tableDesc);
            if (tableExt != null) {
                dumpResources.add(tableExt.getResourcePath());
            }
        });

        KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(kylinConfig, project);
        kafkaConfigManager.listAllKafkaConfigs()
                .forEach(kafkaConfig -> dumpResources.add(kafkaConfig.getResourcePath()));

        dataModels.forEach(dataModel -> {
            mockupResources.put(dataModel.getResourcePath(), dataModel);
            dataModel.setComputedColumnUuids(dataModel.getComputedColumnDescs().stream()
                    .map(ComputedColumnDesc::getUuid).collect(Collectors.toList()));
            dataModel.getComputedColumnDescs()
                    .forEach(computedColumn -> mockupResources.put(computedColumn.getResourcePath(), computedColumn));

            // now get healthy model list through NDataflowManager.listUnderliningDataModels,
            // then here mockup the dataflow and indexPlan for the dataModel
            mockupDataflowAndIndexPlan(dataModel, mockupResources);
        });
    }

    private void mockupDataflowAndIndexPlan(NDataModel dataModel, Map<String, RootPersistentEntity> mockupResources) {
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(dataModel.getUuid());
        indexPlan.setProject(project);
        indexPlan.setDescription(StringUtils.EMPTY);
        NDataflow dataflow = NDataflow.create(indexPlan, RealizationStatusEnum.ONLINE);
        dataflow.setProject(project);
        mockupResources.put(indexPlan.getResourcePath(), indexPlan);
        mockupResources.put(dataflow.getResourcePath(), dataflow);
    }
}
