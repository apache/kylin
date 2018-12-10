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

package org.apache.kylin.stream.core.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ListIterator;
import java.util.Set;

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.DynamicTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * only take effect when the compare filter has function
 */
public class StreamingBuiltInFunctionTransformer implements ITupleFilterTransformer {
    public static final Logger logger = LoggerFactory.getLogger(StreamingBuiltInFunctionTransformer.class);

    private Set<TblColRef> unEvaluableColumns;

    public StreamingBuiltInFunctionTransformer(Set<TblColRef> unEvaluableColumns) {
        this.unEvaluableColumns = unEvaluableColumns;
    }

    @Override
    public TupleFilter transform(TupleFilter tupleFilter) {
        TupleFilter translated = null;
        if (tupleFilter instanceof CompareTupleFilter) {
            //normal case
            translated = translateCompareTupleFilter((CompareTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "}");
            }
        } else if (tupleFilter instanceof BuiltInFunctionTupleFilter) {
            //like case
            translated = translateFunctionTupleFilter((BuiltInFunctionTupleFilter) tupleFilter);
            if (translated != null) {
                logger.info("Translated {" + tupleFilter + "}");
            }
        } else if (tupleFilter instanceof LogicalTupleFilter) {
            @SuppressWarnings("unchecked")
            ListIterator<TupleFilter> childIterator = (ListIterator<TupleFilter>) tupleFilter.getChildren()
                    .listIterator();
            while (childIterator.hasNext()) {
                TupleFilter transformed = transform(childIterator.next());
                if (transformed != null)
                    childIterator.set(transformed);
            }
        }

        TupleFilter result = translated == null ? tupleFilter : translated;
        if (result.getOperator() == TupleFilter.FilterOperatorEnum.NOT
                && !TupleFilter.isEvaluableRecursively(result)) {
            TupleFilter.collectColumns(result, unEvaluableColumns);
            return ConstantTupleFilter.TRUE;
        }

        // shortcut for unEvaluatable filter
        if (!result.isEvaluable()) {
            TupleFilter.collectColumns(result, unEvaluableColumns);
            return ConstantTupleFilter.TRUE;
        }
        return result;
    }

    private TupleFilter translateFunctionTupleFilter(BuiltInFunctionTupleFilter builtInFunctionTupleFilter) {
        if (!builtInFunctionTupleFilter.isValid())
            return null;

        return new EvaluableBuildInFuncTupleFilter(builtInFunctionTupleFilter);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private TupleFilter translateCompareTupleFilter(CompareTupleFilter compTupleFilter) {
        if (compTupleFilter.getFunction() == null
                || (!(compTupleFilter.getFunction() instanceof BuiltInFunctionTupleFilter)))
            return null;

        BuiltInFunctionTupleFilter buildInFunctionTupleFilter = (BuiltInFunctionTupleFilter) compTupleFilter
                .getFunction();
        if (!buildInFunctionTupleFilter.isValid())
            return null;

        return new BuildInFuncCompareTupleFilter(compTupleFilter);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static class BuildInFuncCompareTupleFilter extends TupleFilter {
        private CompareTupleFilter delegate;
        private BuiltInFunctionTupleFilter buildInFunctionTupleFilter;

        protected BuildInFuncCompareTupleFilter(CompareTupleFilter delegate) {
            super(new ArrayList<>(delegate.getChildren()), delegate.getOperator());
            this.delegate = delegate;
            this.buildInFunctionTupleFilter = (BuiltInFunctionTupleFilter) delegate.getFunction();
        }

        @Override
        public boolean isEvaluable() {
            return true;
        }

        @Override
        public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem cs) {
            // extract tuple value
            Object tupleValue = null;
            for (TupleFilter filter : this.buildInFunctionTupleFilter.getChildren()) {
                if (!isConstant(filter)) {
                    filter.evaluate(tuple, cs);
                    tupleValue = filter.getValues().iterator().next();
                }
            }

            // consider null case
            if (cs.isNull(tupleValue)) {
                if (operator == FilterOperatorEnum.ISNULL)
                    return true;
                else
                    return false;
            }

            if (operator == FilterOperatorEnum.ISNOTNULL) {
                return true;
            }
            if (delegate.getFirstValue() == null || cs.isNull(delegate.getFirstValue())) {
                return false;
            }

            // tricky here -- order is ensured by string compare (even for number columns)
            // because it's row key ID (not real value) being compared
            Object computedVal = tupleValue;
            try {
                computedVal = buildInFunctionTupleFilter.invokeFunction(tupleValue);
            } catch (Exception e) {
                logger.error("exception when invoke buildIn function", e);
            }
            int comp = cs.compare(computedVal, delegate.getFirstValue());

            boolean result;
            switch (operator) {
            case EQ:
                result = comp == 0;
                break;
            case NEQ:
                result = comp != 0;
                break;
            case LT:
                result = comp < 0;
                break;
            case LTE:
                result = comp <= 0;
                break;
            case GT:
                result = comp > 0;
                break;
            case GTE:
                result = comp >= 0;
                break;
            case IN:
                result = delegate.getValues().contains(computedVal);
                break;
            case NOTIN:
                result = !delegate.getValues().contains(computedVal);
                break;
            default:
                result = false;
            }
            return result;
        }

        private boolean isConstant(TupleFilter filter) {
            return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
        }

        @Override
        public Collection<?> getValues() {
            return delegate.getValues();
        }

        @Override
        public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }
    }

    private static class EvaluableBuildInFuncTupleFilter extends TupleFilter {
        private BuiltInFunctionTupleFilter buildInFunctionTupleFilter;

        protected EvaluableBuildInFuncTupleFilter(BuiltInFunctionTupleFilter builtInFunctionTupleFilter) {
            super(new ArrayList<>(builtInFunctionTupleFilter.getChildren()), builtInFunctionTupleFilter.getOperator());
            this.buildInFunctionTupleFilter = builtInFunctionTupleFilter;
        }

        @Override
        public boolean isEvaluable() {
            return true;
        }

        @Override
        public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem cs) {
            // extract tuple value
            Object tupleValue = null;
            for (TupleFilter filter : this.children) {
                if (!isConstant(filter)) {
                    filter.evaluate(tuple, cs);
                    tupleValue = filter.getValues().iterator().next();
                }
            }
            if (tupleValue == null || cs.isNull(tupleValue)) {
                return false;
            }

            try {
                return (Boolean) buildInFunctionTupleFilter.invokeFunction(tupleValue);
            } catch (Exception e) {
                logger.error("error when invoke build in function", e);
                return false;
            }
        }

        @Override
        public Collection<?> getValues() {
            return null;
        }

        @Override
        public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        private boolean isConstant(TupleFilter filter) {
            return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
        }
    }
}