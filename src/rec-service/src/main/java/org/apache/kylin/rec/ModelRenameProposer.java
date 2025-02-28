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

package org.apache.kylin.rec;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelRenameProposer extends AbstractProposer {

    private static final String MODEL_ALIAS_PREFIX = "AUTO_MODEL_";
    public static final String MODEL_ALIAS_TRUNCATE_SUFFIX = "_TRUNC";

    public ModelRenameProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {

        Set<String> usedNames = getAllModelNames();

        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        for (AbstractContext.ModelContext modelCtx : modelContexts) {
            if (modelCtx.isTargetModelMissing()) {
                continue;
            }

            NDataModel targetModel = modelCtx.getTargetModel();
            String alias = modelCtx.getOriginModel() == null //
                    ? proposeModelAlias(targetModel, usedNames) //
                    : modelCtx.getOriginModel().getAlias();
            targetModel.setAlias(alias);
        }
    }

    private Set<String> getAllModelNames() {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(),
                project);
        Set<String> modelNames = Sets.newHashSet();
        modelNames.addAll(dataModelManager.listAllModelAlias());
        modelNames.addAll(getProposeContext().getExtraMeta().getAllModels());
        return modelNames;
    }

    private String proposeModelAlias(NDataModel model, Set<String> usedModelNames) {
        Set<String> usedNames = usedModelNames.stream().map(name -> name.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());
        String rootTableAlias = model.getRootFactTable().getAlias();
        int suffix = 0;
        String targetName;
        String truncateSuffix;
        int truncatePosition;
        do {
            if (suffix++ < 0) {
                throw new IllegalStateException("Potential infinite loop in getModelName().");
            }
            if (StringUtils.isNotEmpty(proposeContext.getModelName())) {
                targetName = proposeContext.getModelName() + "_" + suffix;
            } else {
                targetName = ModelRenameProposer.MODEL_ALIAS_PREFIX + rootTableAlias + "_" + suffix;
            }

            if (targetName.length() > Constant.MODEL_ALIAS_LEN_LIMIT) {
                truncateSuffix = MODEL_ALIAS_TRUNCATE_SUFFIX + "_" + suffix;
                truncatePosition = Math.max(Constant.MODEL_ALIAS_LEN_LIMIT - truncateSuffix.length(), 0);
                targetName = targetName.substring(0, truncatePosition) + truncateSuffix;
            }
        } while (usedNames.contains(targetName.toUpperCase(Locale.ROOT)));
        log.info("The alias of the model({}) was rename to {}.", model.getId(), targetName);
        usedModelNames.add(targetName.toUpperCase(Locale.ROOT));
        return targetName;
    }

    @Override
    public String getIdentifierName() {
        return "ModelRenameProposer";
    }
}
