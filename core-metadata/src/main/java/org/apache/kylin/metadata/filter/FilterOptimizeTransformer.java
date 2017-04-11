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
 * optimize the filter if possible, not limited to:
 * <p>
 * 1. prune filters like (a = ? OR 1 = 1)
 */
public class FilterOptimizeTransformer implements ITupleFilterTransformer {
    public static final Logger logger = LoggerFactory.getLogger(FilterOptimizeTransformer.class);

    @Override
    public TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            //normal case
            translated = replaceAlwaysTrueCompareFilter((CompareTupleFilter) tupleFilter);
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

            translated = replaceAlwaysTrueLogicalFilter((LogicalTupleFilter) tupleFilter);

        }
        return translated == null ? tupleFilter : translated;

    }

    private TupleFilter replaceAlwaysTrueLogicalFilter(LogicalTupleFilter logicalTupleFilter) {
        if (logicalTupleFilter == null) {
            return null;
        }

        if (logicalTupleFilter.getOperator() == TupleFilter.FilterOperatorEnum.OR) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) logicalTupleFilter.getChildren().listIterator();
            while (childIterator.hasNext()) {
                TupleFilter next = childIterator.next();
                if (ConstantTupleFilter.TRUE == next) {
                    logger.debug("Translated {{}} to ConstantTupleFilter.TRUE", logicalTupleFilter);
                    return ConstantTupleFilter.TRUE;
                }
            }
        }

        return logicalTupleFilter;
    }

    private TupleFilter replaceAlwaysTrueCompareFilter(CompareTupleFilter compareTupleFilter) {

        if (compareTupleFilter != null && compareTupleFilter.alwaysReturnTrue()) {
            logger.debug("Translated {{}} to ConstantTupleFilter.TRUE", compareTupleFilter);
            return ConstantTupleFilter.TRUE;
        }

        return compareTupleFilter;
    }

}
