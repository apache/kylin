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

package org.apache.kylin.metadata.filter;

import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class EvaluatableLikeFunctionTransformer {
    public static final Logger logger = LoggerFactory.getLogger(EvaluatableLikeFunctionTransformer.class);

    public static TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            CompareTupleFilter compTupleFilter = (CompareTupleFilter) tupleFilter;
            if (compTupleFilter.getFunction() != null && (compTupleFilter.getFunction() instanceof BuiltInFunctionTupleFilter)) {
                throw new IllegalArgumentException("BuiltInFunctionTupleFilter not supported :" + ((BuiltInFunctionTupleFilter) compTupleFilter.getFunction()).getName());
            }
        } else if (tupleFilter instanceof BuiltInFunctionTupleFilter) {
            BuiltInFunctionTupleFilter builtInFunctionTupleFilter = (BuiltInFunctionTupleFilter) tupleFilter;
            if (isLikeFunction(builtInFunctionTupleFilter)) {
                for (TupleFilter child : builtInFunctionTupleFilter.getChildren()) {
                    if (!(child instanceof ColumnTupleFilter) && !(child instanceof ConstantTupleFilter)) {
                        throw new IllegalArgumentException("Only simple like clause is supported");
                    }
                }

                translated = new EvaluatableLikeFunction(builtInFunctionTupleFilter.getName());
                for (TupleFilter child : builtInFunctionTupleFilter.getChildren()) {
                    translated.addChild(child);
                }

            } else {
                throw new IllegalArgumentException("BuiltInFunctionTupleFilter not supported: " + builtInFunctionTupleFilter.getName());
            }

        } else if (tupleFilter instanceof LogicalTupleFilter) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren().listIterator();
            while (childIterator.hasNext()) {
                TupleFilter transformed = transform(childIterator.next());
                if (transformed != null) {
                    childIterator.set(transformed);
                } else {
                    throw new IllegalStateException("Should not be null");
                }
            }
        }
        return translated == null ? tupleFilter : translated;
    }

    private static boolean isLikeFunction(BuiltInFunctionTupleFilter builtInFunctionTupleFilter) {
        return "like".equalsIgnoreCase(builtInFunctionTupleFilter.getName());
    }

}