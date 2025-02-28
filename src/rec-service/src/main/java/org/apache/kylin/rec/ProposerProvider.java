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

public class ProposerProvider {
    private AbstractContext proposeContext;

    private ProposerProvider(AbstractContext smartContext) {
        this.proposeContext = smartContext;
    }

    public static ProposerProvider create(AbstractContext smartContext) {
        return new ProposerProvider(smartContext);
    }

    public AbstractProposer getSQLAnalysisProposer() {
        return new SQLAnalysisProposer(proposeContext);
    }

    public AbstractProposer getModelSelectProposer() {
        return new ModelSelectProposer(proposeContext);
    }

    public AbstractProposer getModelOptProposer() {
        return new ModelOptProposer(proposeContext);
    }

    public AbstractProposer getModelInfoAdjustProposer() {
        return new ModelInfoAdjustProposer(proposeContext);
    }

    public AbstractProposer getIndexPlanSelectProposer() {
        return new IndexPlanSelectProposer(proposeContext);
    }

    public AbstractProposer getIndexPlanOptProposer() {
        return new IndexPlanOptProposer(proposeContext);
    }

    public AbstractProposer getIndexPlanShrinkProposer() {
        return new IndexPlanShrinkProposer(proposeContext);
    }

    public AbstractProposer getModelRenameProposer() {
        return new ModelRenameProposer(proposeContext);
    }
}
