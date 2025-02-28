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

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.rec.index.IndexMaster;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexPlanOptProposer extends AbstractProposer {

    public IndexPlanOptProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        if (proposeContext.getModelContexts() == null) {
            return;
        }

        for (AbstractContext.ModelContext modelCtx : proposeContext.getModelContexts()) {
            IndexMaster indexMaster = new IndexMaster(modelCtx);
            if (modelCtx.isTargetModelMissing() || modelCtx.isSnapshotSelected()) {
                continue;
            }

            try {
                IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
                if (indexPlan == null) {
                    indexPlan = indexMaster.proposeInitialIndexPlan();
                }

                indexPlan = indexMaster.proposeCuboids(indexPlan);
                modelCtx.setTargetIndexPlan(indexPlan);
            } catch (Exception e) {
                log.error("[UNLIKELY_THINGS_HAPPENED] Something wrong occurs in initialize target indexPlan.", e);
                modelCtx.setTargetIndexPlan(null);
                proposeContext.recordException(modelCtx, e);
            }
        }
    }

    @Override
    public String getIdentifierName() {
        return "IndexPlanOptProposer";
    }
}
