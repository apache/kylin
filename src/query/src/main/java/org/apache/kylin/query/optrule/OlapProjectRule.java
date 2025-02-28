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

package org.apache.kylin.query.optrule;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.kylin.query.relnode.OlapProjectRel;
import org.apache.kylin.query.relnode.OlapRel;

public class OlapProjectRule extends ConverterRule {

    public static final RelOptRule INSTANCE = new OlapProjectRule();

    public OlapProjectRule() {
        super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE, OlapRel.CONVENTION,
                "OlapProjectRule");
    }

    @Override
    public RelNode convert(final RelNode rel) {

        final LogicalProject project = (LogicalProject) rel;

        final RelNode convertedInput = project.getInput() instanceof HepRelVertex ? project.getInput()
                : convert(project.getInput(), project.getInput().getTraitSet().replace(OlapRel.CONVENTION));
        final RelOptCluster cluster = convertedInput.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet().replace(OlapRel.CONVENTION).replaceIfs(
                RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project(mq, convertedInput, project.getProjects()));

        return new OlapProjectRel(convertedInput.getCluster(), traitSet, convertedInput, project.getProjects(),
                project.getRowType());
    }
}
