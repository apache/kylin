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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.util.Collection;
import java.util.Set;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.BuiltInFunctionTransformer;
import org.apache.kylin.dict.DictCodeSystem;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("unchecked")
public class FilterDecorator implements TupleFilterSerializer.Decorator {
    public enum FilterConstantsTreatment {
        AS_IT_IS, REPLACE_WITH_GLOBAL_DICT, REPLACE_WITH_LOCAL_DICT
    }

    private IDimensionEncodingMap dimEncMap;
    private RowKeyColumnIO columnIO;
    private Set<TblColRef> inevaluableColumns;
    private FilterConstantsTreatment filterConstantsTreatment;

    public FilterDecorator(IDimensionEncodingMap dimEncMap, FilterConstantsTreatment filterConstantsTreatment) {
        this.dimEncMap = dimEncMap;
        this.columnIO = new RowKeyColumnIO(dimEncMap);
        this.inevaluableColumns = Sets.newHashSet();
        this.filterConstantsTreatment = filterConstantsTreatment;
    }

    public Set<TblColRef> getInevaluableColumns() {
        return inevaluableColumns;
    }

    private TupleFilter replaceConstantsWithLocalDict(CompareTupleFilter oldCompareFilter, CompareTupleFilter newCompareFilter) {
        //TODO localdict: (performance issue) transalte() with roundingflag 0 will use try catch exceptions to deal with non-existing entries
        return replaceConstantsWithGlobalDict(oldCompareFilter, newCompareFilter);
    }

    private TupleFilter replaceConstantsWithGlobalDict(CompareTupleFilter oldCompareFilter, CompareTupleFilter newCompareFilter) {
        Collection<String> constValues = (Collection<String>) oldCompareFilter.getValues();
        String firstValue = constValues.iterator().next();
        TblColRef col = newCompareFilter.getColumn();

        TupleFilter result;
        String v;

        // translate constant into rowkey ID
        switch (newCompareFilter.getOperator()) {
        case EQ:
        case IN:
            Set<String> newValues = Sets.newHashSet();
            for (String value : constValues) {
                v = translate(col, value, 0);
                if (!isDictNull(v))
                    newValues.add(v);
            }
            if (newValues.isEmpty()) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(newValues));
                result = newCompareFilter;
            }
            break;
        case NEQ:
            v = translate(col, firstValue, 0);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case LT:
            v = translate(col, firstValue, 1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case LTE:
            v = translate(col, firstValue, -1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case GT:
            v = translate(col, firstValue, -1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.TRUE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        case GTE:
            v = translate(col, firstValue, 1);
            if (isDictNull(v)) {
                result = ConstantTupleFilter.FALSE;
            } else {
                newCompareFilter.addChild(new ConstantTupleFilter(v));
                result = newCompareFilter;
            }
            break;
        default:
            throw new IllegalStateException("Cannot handle operator " + newCompareFilter.getOperator());
        }
        return result;
    }

    private boolean isDictNull(String v) {
        return DictCodeSystem.INSTANCE.isNull(v);
    }

    @Override
    public TupleFilter onSerialize(TupleFilter filter) {
        if (filter == null)
            return null;

        BuiltInFunctionTransformer translator = new BuiltInFunctionTransformer(dimEncMap);
        filter = translator.transform(filter);

        // un-evaluatable filter is replaced with TRUE
        if (!filter.isEvaluable()) {
            TupleFilter.collectColumns(filter, inevaluableColumns);
            return ConstantTupleFilter.TRUE;
        }

        if (!(filter instanceof CompareTupleFilter))
            return filter;

        // double check all internal of CompareTupleFilter is evaluatable
        if (!TupleFilter.isEvaluableRecursively(filter)) {
            TupleFilter.collectColumns(filter, inevaluableColumns);
            return ConstantTupleFilter.TRUE;
        }

        if (filterConstantsTreatment == FilterConstantsTreatment.AS_IT_IS) {
            return filter;
        } else {

            // extract ColumnFilter & ConstantFilter
            CompareTupleFilter compareFilter = (CompareTupleFilter) filter;
            TblColRef col = compareFilter.getColumn();

            if (col == null) {
                return filter;
            }

            Collection<String> constValues = (Collection<String>) compareFilter.getValues();
            if (constValues == null || constValues.isEmpty()) {
                return filter;
            }

            CompareTupleFilter newCompareFilter = new CompareTupleFilter(compareFilter.getOperator());
            newCompareFilter.addChild(new ColumnTupleFilter(col));

            if (filterConstantsTreatment == FilterConstantsTreatment.REPLACE_WITH_GLOBAL_DICT) {
                return replaceConstantsWithGlobalDict(compareFilter, newCompareFilter);
            } else if (filterConstantsTreatment == FilterConstantsTreatment.REPLACE_WITH_LOCAL_DICT) {
                return replaceConstantsWithLocalDict(compareFilter, newCompareFilter);
            } else {
                throw new RuntimeException("should not reach here");
            }
        }
    }

    private String translate(TblColRef column, String v, int roundingFlag) {
        byte[] value = Bytes.toBytes(v);
        byte[] id = new byte[dimEncMap.get(column).getLengthOfEncoding()];
        columnIO.writeColumn(column, value, value.length, roundingFlag, DimensionEncoding.NULL, id, 0);
        return Dictionary.dictIdToString(id, 0, id.length);
    }
}