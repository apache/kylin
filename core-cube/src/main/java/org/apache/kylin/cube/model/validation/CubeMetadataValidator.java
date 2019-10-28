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

package org.apache.kylin.cube.model.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.rule.AggregationGroupRule;
import org.apache.kylin.cube.model.validation.rule.DictionaryRule;
import org.apache.kylin.cube.model.validation.rule.FunctionRule;
import org.apache.kylin.cube.model.validation.rule.RowKeyAttrRule;
import org.apache.kylin.cube.model.validation.rule.StreamingCubeRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For cube metadata validator
 * 
 * @author jianliu
 * 
 */
public class CubeMetadataValidator {
    protected static final Logger logger = LoggerFactory.getLogger(CubeMetadataValidator.class);
    @SuppressWarnings("unchecked")
    private IValidatorRule<CubeDesc>[] defaultRules = new IValidatorRule[] { new FunctionRule(),
            new AggregationGroupRule(), new RowKeyAttrRule(), new DictionaryRule(), new StreamingCubeRule() };

    private List<IValidatorRule<CubeDesc>> rules;

    public CubeMetadataValidator(KylinConfig config) {
        rules = new ArrayList<>(Arrays.asList(defaultRules));
        for (String ruleName : config.getCubeMetadataExtraValidators()) {
            try {
                IValidatorRule<CubeDesc> rule = (IValidatorRule<CubeDesc>) Class.forName(ruleName).getConstructor()
                        .newInstance();
                rules.add(rule);
            } catch (Exception e) {
                logger.error("Construct cube metadata validator rule: {} failed. Ignore this rule", ruleName, e);
            }
        }
    }

    public ValidateContext validate(CubeDesc cube) {
        ValidateContext context = new ValidateContext();
        for (IValidatorRule<CubeDesc> rule : rules) {
            rule.validate(cube, context);
        }

        for (ValidateContext.Result result : context.getResults()) {
            cube.addError(result.getLevel() + " : " + result.getMessage());
        }
        return context;
    }

}
