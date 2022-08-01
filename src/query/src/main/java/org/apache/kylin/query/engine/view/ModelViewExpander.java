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

package org.apache.kylin.query.engine.view;

import java.util.List;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.query.relnode.KapModelViewRel;

public class ModelViewExpander implements RelOptTable.ViewExpander {

    private SimpleViewExpander expander;

    public ModelViewExpander(SimpleViewExpander expander) {
        this.expander = expander;
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        RelRoot root = expander.expandView(rowType, queryString, schemaPath, viewPath);
        KapModelViewRel modelViewRel = new KapModelViewRel(root.rel.getCluster(), root.rel.getTraitSet(), root.rel,
                viewPath.get(1));
        return RelRoot.of(modelViewRel, root.kind);
    }
}
