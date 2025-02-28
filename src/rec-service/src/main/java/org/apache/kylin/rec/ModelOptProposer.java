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

import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.model.ModelMaster;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelOptProposer extends AbstractProposer {

    public static final String NO_COMPATIBLE_MODEL_MSG = "There is no compatible model to accelerate this sql.";

    public ModelOptProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        if (proposeContext.getModelContexts() == null) {
            return;
        }

        for (AbstractContext.ModelContext modelCtx : proposeContext.getModelContexts()) {
            ModelMaster modelMaster = new ModelMaster(modelCtx);

            if (modelCtx.isSnapshotSelected()) {
                continue;
            }

            try {
                NDataModel model = modelCtx.getTargetModel();
                model = modelMaster.proposeJoins(model);
                model = modelMaster.proposeComputedColumn(model);
                model = modelMaster.proposeScope(model);
                model = modelMaster.shrinkComputedColumn(model);
                modelCtx.setTargetModel(model);
            } catch (Exception e) {
                log.error("Unexpected exception occurs in initialize target model.", e);
                modelCtx.setTargetModel(null);
                proposeContext.recordException(modelCtx, e);
            }
        }
    }

    @Override
    public String getIdentifierName() {
        return "ModelOptProposer";
    }
}
