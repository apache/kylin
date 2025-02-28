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

package org.apache.kylin.rec.model;

import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelReuseContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProposerProvider {

    private ProposerProvider(AbstractContext.ModelContext modelContext) {
        this.modelContext = modelContext;
    }

    private AbstractContext.ModelContext modelContext;

    public static ProposerProvider create(AbstractContext.ModelContext modelContext) {
        return new ProposerProvider(modelContext);
    }

    public AbstractModelProposer getJoinProposer() {
        return new JoinProposer(modelContext);
    }

    public AbstractModelProposer getScopeProposer() {
        return new QueryScopeProposer(modelContext);
    }

    public AbstractModelProposer getComputedColumnProposer() {
        return modelContext.getProposeContext() instanceof ModelReuseContext
                ? new ComputedColumnProposer.ComputedColumnProposerOfModelReuseContext(modelContext)
                : new ComputedColumnProposer(modelContext);
    }

    public AbstractModelProposer getShrinkComputedColumnProposer() {
        return new ShrinkComputedColumnProposer(modelContext);
    }
}
