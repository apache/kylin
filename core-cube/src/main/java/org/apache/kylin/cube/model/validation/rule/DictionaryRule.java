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

package org.apache.kylin.cube.model.validation.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dict.GlobalDictionaryBuilder;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Validate Dictionary Settings:
 *
 * <ul>
 *     <li> no duplicated dictionary for one column
 *     <li> dictionary can't set both `reuse` and `builder`
 *     <li> transitive `reuse` like "a <- b <- c" is not allowed, force "a <- b, a <- c"
 * </ul>
 */
public class DictionaryRule implements IValidatorRule<CubeDesc> {
    static final String ERROR_DUPLICATE_DICTIONARY_COLUMN = "Duplicated dictionary specification for column: ";
    static final String ERROR_REUSE_BUILDER_BOTH_SET = "REUSE and BUILDER both set on dictionary for column: ";
    static final String ERROR_REUSE_BUILDER_BOTH_EMPTY = "REUSE and BUILDER both empty on dictionary for column: ";
    static final String ERROR_TRANSITIVE_REUSE = "Transitive REUSE is not allowed for dictionary: ";
    static final String ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE = "Global dictionary couldn't be used for dimension column: ";

    @Override
    public void validate(CubeDesc cubeDesc, ValidateContext context) {
        List<DictionaryDesc> dictDescs = cubeDesc.getDictionaries();
        Set<TblColRef> dimensionColumns = cubeDesc.listDimensionColumnsIncludingDerived();

        if (dictDescs == null || dictDescs.isEmpty()) {
            return;
        }

        Set<TblColRef> allDictCols = new HashSet<>();
        Set<TblColRef> baseCols = new HashSet<>(); // col with builder
        List<DictionaryDesc> reuseDictionaries = new ArrayList<>();

        // first pass
        for (DictionaryDesc dictDesc : dictDescs) {
            TblColRef dictCol = dictDesc.getColumnRef();
            TblColRef reuseCol = dictDesc.getResuseColumnRef();
            String builderClass = dictDesc.getBuilderClass();

            if (!allDictCols.add(dictCol)) {
                context.addResult(ResultLevel.ERROR, ERROR_DUPLICATE_DICTIONARY_COLUMN + dictCol);
                return;
            }

            if (reuseCol != null && StringUtils.isNotEmpty(builderClass)) {
                context.addResult(ResultLevel.ERROR, ERROR_REUSE_BUILDER_BOTH_SET + dictCol);
                return;
            }

            if (reuseCol == null && StringUtils.isEmpty(builderClass)) {
                context.addResult(ResultLevel.ERROR, ERROR_REUSE_BUILDER_BOTH_EMPTY + dictCol);
                return;
            }

            if (StringUtils.isNotEmpty(builderClass) && builderClass.equalsIgnoreCase(GlobalDictionaryBuilder.class.getName()) && dimensionColumns.contains(dictCol)) {
                context.addResult(ResultLevel.ERROR, ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE + dictCol);
                return;
            }

            if (reuseCol != null) {
                reuseDictionaries.add(dictDesc);
            } else {
                baseCols.add(dictCol);
            }
        }

        // second pass: check no transitive reuse
        for (DictionaryDesc dictDesc : reuseDictionaries) {
            if (!baseCols.contains(dictDesc.getResuseColumnRef())) {
                context.addResult(ResultLevel.ERROR, ERROR_TRANSITIVE_REUSE + dictDesc.getColumnRef());
                return;
            }
        }
    }
}
