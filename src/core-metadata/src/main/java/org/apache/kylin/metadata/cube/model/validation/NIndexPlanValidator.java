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

package org.apache.kylin.metadata.cube.model.validation;

import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cube.model.IndexPlan;

public class NIndexPlanValidator {
    @SuppressWarnings("unchecked")
    private IValidatorRule<IndexPlan>[] rules = new IValidatorRule[0]; // TODO: add more rules

    public ValidateContext validate(IndexPlan cube) {
        ValidateContext context = new ValidateContext();
        for (IValidatorRule<IndexPlan> rule : rules) {
            rule.validate(cube, context);
        }

        for (ValidateContext.Result result : context.getResults()) {
            cube.addError(result.getLevel() + " : " + result.getMessage());
        }
        return context;
    }
}
