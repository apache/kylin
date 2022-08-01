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
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.metadata.cube.model.IndexEntity.TABLE_INDEX_START_ID;
import static org.apache.kylin.tool.util.MetadataUtil.getMetadataUrl;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnGreen;
import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.base.Preconditions;

import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * 4.1 -> 4.2
 */
@Slf4j
public class UpdateModelCLI extends ExecutableApplication {

    private static final Option OPTION_METADATA_DIR = OptionBuilder.getInstance().hasArg().withArgName("metadata_dir")
            .withDescription("metadata dir.").isRequired(true).withLongOpt("metadata_dir").create("d");

    private static final Option OPTION_EXEC = OptionBuilder.getInstance().hasArg(false).withArgName("exec")
            .withDescription("exec the upgrade.").isRequired(false).withLongOpt("exec").create("e");

    public static void main(String[] args) {
        UpdateModelCLI updateModelCLI = new UpdateModelCLI();
        try {
            updateModelCLI.execute(args);
        } catch (Exception e) {
            log.error("Failed to exec UpdateModelCLI", e);
            systemExitWhenMainThread(1);
        }

        log.info("Upgrade model finished.");
        systemExitWhenMainThread(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_METADATA_DIR);
        options.addOption(OPTION_EXEC);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_METADATA_DIR));
        Preconditions.checkArgument(StringUtils.isNotBlank(metadataUrl));

        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        systemKylinConfig.setMetadataUrl(metadataUrl);

        log.info("Start to upgrade all model.");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        List<NDataModel> globalUpdateModelList = Lists.newArrayList();

        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.listAllProjects().forEach(projectInstance -> {
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, projectInstance.getName());

            NDataModelManager dataModelManager = NDataModelManager.getInstance(kylinConfig, projectInstance.getName());
            List<NDataModel> updateModelList = dataModelManager.listAllModels().stream()
                    .filter(nDataModel -> !nDataModel.isBroken())
                    .filter(nDataModel -> !indexPlanManager.getIndexPlan(nDataModel.getUuid()).isBroken())
                    .map(dataModelManager::copyForWrite).peek(model -> {
                        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getUuid());

                        Set<Integer> tableIndexDimensions = indexPlan.getIndexes().stream()
                                .filter(indexEntity -> indexEntity.getId() >= TABLE_INDEX_START_ID)
                                .flatMap(indexEntity -> indexEntity.getDimensions().stream())
                                .collect(Collectors.toSet());
                        long count = model.getAllNamedColumns().stream().filter(namedColumn -> {
                            if (!namedColumn.isDimension() && tableIndexDimensions.contains(namedColumn.getId())) {
                                namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
                                return true;
                            }
                            return false;
                        }).count();

                        log.info("Project: {}, model: {}, add model dimension size: {}", projectInstance.getName(),
                                model.getAlias(), count);
                    }).collect(Collectors.toList());

            globalUpdateModelList.addAll(updateModelList);
        });

        printlnGreen(String.format(Locale.ROOT, "found %d models need to be modified.", globalUpdateModelList.size()));
        if (optionsHelper.hasOption(OPTION_EXEC)) {

            Map<String, List<NDataModel>> modelsGroupByProject = globalUpdateModelList.stream()
                    .collect(Collectors.groupingBy(NDataModel::getProject));

            modelsGroupByProject.forEach((project, models) -> UnitOfWork.doInTransactionWithRetry(() -> {
                models.forEach(model -> NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .updateDataModelDesc(model));
                return null;
            }, project));
            printlnGreen("model dimensions upgrade succeeded.");
        }

        log.info("Succeed to upgrade all model.");
    }
}
