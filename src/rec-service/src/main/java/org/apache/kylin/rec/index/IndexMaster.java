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

package org.apache.kylin.rec.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.rec.AbstractContext;

public class IndexMaster {

    private final AbstractContext.ModelContext context;
    private final ProposerProvider proposerProvider;

    public IndexMaster(AbstractContext.ModelContext context) {
        this.context = context;
        this.proposerProvider = ProposerProvider.create(this.context);
    }

    public AbstractContext.ModelContext getContext() {
        return this.context;
    }

    public IndexPlan proposeInitialIndexPlan() {
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(context.getTargetModel().getUuid());
        indexPlan.setDescription(StringUtils.EMPTY);
        return indexPlan;
    }

    public IndexPlan proposeCuboids(final IndexPlan indexPlan) {
        return proposerProvider.getCuboidProposer().execute(indexPlan);
    }

    public IndexPlan reduceCuboids(final IndexPlan indexPlan) {
        return proposerProvider.getCuboidReducer().execute(indexPlan);
    }
}
