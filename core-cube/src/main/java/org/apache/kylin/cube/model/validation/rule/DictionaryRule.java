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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dict.GlobalDictionaryBuilder;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
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
    static final String ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE = "If one column is used for both dimension and precisely count distinct measure, its dimension encoding should not be dict: ";
    static final String ERROR_GLOBAL_DICTIONNARY_FOR_BITMAP_MEASURE = "For bitmap based count distinct column (as the data type is not int), a Global dictionary is required: ";
    static final String ERROR_REUSE_GLOBAL_DICTIONNARY_FOR_BITMAP_MEASURE = "If one bitmap based count distinct column (as the data type is not int) REUSE another column, a Global dictionary is required: ";

    @Override
    public void validate(CubeDesc cubeDesc, ValidateContext context) {
        List<DictionaryDesc> dictDescs = cubeDesc.getDictionaries();
        Set<TblColRef> dimensionColumns = cubeDesc.listDimensionColumnsIncludingDerived();
        RowKeyDesc rowKeyDesc = cubeDesc.getRowkey();

        if (dictDescs == null || dictDescs.isEmpty()) {
            return;
        }

        Set<TblColRef> allDictCols = new HashSet<>();
        Map<TblColRef, DictionaryDesc> baseCols = new HashMap<>(); // col with builder
        List<DictionaryDesc> reuseDictionaries = new ArrayList<>();
        Map<TblColRef, MeasureDesc> bitmapMeasures = new HashMap<>();
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()){
            if (measureDesc.getFunction().getMeasureType() instanceof BitmapMeasureType)
                bitmapMeasures.put(measureDesc.getFunction().getParameter().getColRef(), measureDesc);
        }

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

            if (StringUtils.isNotEmpty(builderClass) && builderClass.equalsIgnoreCase(GlobalDictionaryBuilder.class.getName()) && dimensionColumns.contains(dictCol) && rowKeyDesc.isUseDictionary(dictCol)) {
                context.addResult(ResultLevel.ERROR, ERROR_GLOBAL_DICTIONNARY_ONLY_MEASURE + dictCol);
                return;
            }

            if (StringUtils.isNotEmpty(builderClass) && !builderClass.equalsIgnoreCase(GlobalDictionaryBuilder.class.getName()) && bitmapMeasures.containsKey(dictCol) && !dictCol.getColumnDesc().getType().isIntegerFamily()){
                context.addResult(ResultLevel.ERROR, ERROR_GLOBAL_DICTIONNARY_FOR_BITMAP_MEASURE + dictCol);
                return;
            }

            if (reuseCol != null) {
                reuseDictionaries.add(dictDesc);
            } else {
                baseCols.put(dictCol, dictDesc);
            }
        }

        // second pass: check no transitive reuse
        for (DictionaryDesc dictDesc : reuseDictionaries) {
            TblColRef dictCol = dictDesc.getColumnRef();

            if (!baseCols.containsKey(dictDesc.getResuseColumnRef())) {
                context.addResult(ResultLevel.ERROR, ERROR_TRANSITIVE_REUSE + dictCol);
                return;
            }

            TblColRef reuseCol = dictDesc.getResuseColumnRef();
            String reuseBuilderClass = baseCols.get(reuseCol).getBuilderClass();

            if (bitmapMeasures.containsKey(dictCol) && !dictCol.getColumnDesc().getType().isIntegerFamily() && !reuseBuilderClass.equalsIgnoreCase(GlobalDictionaryBuilder.class.getName())){
                context.addResult(ResultLevel.ERROR, ERROR_REUSE_GLOBAL_DICTIONNARY_FOR_BITMAP_MEASURE + dictCol);
                return;
            }
        }
    }
}
