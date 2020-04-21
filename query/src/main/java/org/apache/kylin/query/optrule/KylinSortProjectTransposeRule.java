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

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilderFactory;

public class KylinSortProjectTransposeRule extends SortProjectTransposeRule {
    public static final KylinSortProjectTransposeRule INSTANCE =
            new KylinSortProjectTransposeRule(Sort.class, LogicalProject.class,
                    RelFactories.LOGICAL_BUILDER, null);

    private KylinSortProjectTransposeRule(
            Class<? extends Sort> sortClass,
            Class<? extends Project> projectClass,
            RelBuilderFactory relBuilderFactory, String description) {
        super(
                operand(sortClass,
                        operand(projectClass, null, KylinSortProjectTransposeRule::noWindowFunc, any())),
                relBuilderFactory, description);
    }

    private static boolean noWindowFunc(Project project) {
        return !RexOver.containsOver(project.getProjects(), null);
    }
}
