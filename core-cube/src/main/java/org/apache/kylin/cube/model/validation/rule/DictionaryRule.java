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

import java.util.HashMap;
import java.util.List;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dict.GlobalDictionaryBuilder;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by sunyerui on 16/6/1.
 */
public class DictionaryRule implements IValidatorRule<CubeDesc> {

    @Override
    public void validate(CubeDesc cubeDesc, ValidateContext context) {
        List<DictionaryDesc> dictDescs = cubeDesc.getDictionaries();
        if (dictDescs == null || dictDescs.isEmpty()) {
            return;
        }

        HashMap<TblColRef, String> colToBuilderMap = new HashMap<>();
        HashMap<TblColRef, TblColRef> colToReuseColMap = new HashMap<>();
        for (DictionaryDesc dictDesc : dictDescs) {
            // Make sure the same column associate with same builder class, check the reuse column by default
            TblColRef dictCol = dictDesc.getResuseColumnRef();
            if (dictCol == null) {
                dictCol = dictDesc.getColumnRef();
            }
            if (dictCol == null) {
                context.addResult(ResultLevel.ERROR, "Some column in dictionaries not found");
                return;
            }
            String builder = dictDesc.getBuilderClass();
            String oldBuilder = colToBuilderMap.put(dictCol, builder);
            if (oldBuilder != null && !oldBuilder.equals(builder)) {
                context.addResult(ResultLevel.ERROR, "Column " + dictCol + " has inconsistent builders " + builder + " and " + oldBuilder);
                return;
            }

            // Make sure one column only reuse another one column
            if (dictDesc.getResuseColumnRef() != null) {
                TblColRef oldReuseCol = colToReuseColMap.put(dictDesc.getColumnRef(), dictDesc.getResuseColumnRef());
                if (oldReuseCol != null && !dictDesc.getResuseColumnRef().equals(oldReuseCol)) {
                    context.addResult(ResultLevel.ERROR, "Column " + dictDesc.getColumnRef() + " reuse inconsistent column " + dictDesc.getResuseColumnRef() + " and " + oldReuseCol);
                    return;
                }
            }
        }

        // Make sure one column do not use GlobalDictBuilder and DimensionDictEncoding together
        RowKeyColDesc[] rowKeyColDescs = cubeDesc.getRowkey().getRowKeyColumns();
        for (RowKeyColDesc desc : rowKeyColDescs) {
            if (desc.isUsingDictionary()) {
                String builder = colToBuilderMap.get(desc.getColRef());
                if (builder != null && builder.equals(GlobalDictionaryBuilder.class.getName())) {
                    context.addResult(ResultLevel.ERROR, "Column " + desc.getColRef() + " used as dimension and conflict with GlobalDictBuilder");
                    return;
                }
            }
        }

    }
}
