/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.observer;

import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Sets;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.kv.RowKeyColumnIO;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.filter.TupleFilter.FilterOperatorEnum;
import com.kylinolap.storage.filter.TupleFilterSerializer;
import com.kylinolap.storage.filter.TupleFilterSerializer.Decorator;
import com.kylinolap.storage.tuple.ITuple;

/**
 * @author yangli9
 * 
 */
public class SRowFilter {

    public static SRowFilter fromFilter(final CubeSegment seg, TupleFilter rootFilter) {
        // translate constants into dictionary IDs via a serialize copy
        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, new Decorator() {
            RowKeyColumnIO columnIO = new RowKeyColumnIO(seg);

            @Override
            public TupleFilter onSerialize(TupleFilter filter) {
                if (filter == null)
                    return filter;
                
                if (filter.getOperator() == FilterOperatorEnum.NOT && TupleFilter.isEvaluableRecursively(filter) == false)
                    return ConstantTupleFilter.TRUE;

                if ((filter instanceof CompareTupleFilter) == false)
                    return filter;

                if (TupleFilter.isEvaluableRecursively(filter) == false)
                    return ConstantTupleFilter.TRUE;

                // extract ColumnFilter & ConstantFilter
                CompareTupleFilter compf = (CompareTupleFilter) filter;
                TblColRef col = compf.getColumn();
                if (col == null) {
                    return filter;
                }
                String nullString = nullString(col);
                Collection<String> constValues = compf.getValues();
                if (constValues == null || constValues.isEmpty()) {
                    compf.setNullString(nullString); // maybe ISNULL
                    return filter;
                }

                TupleFilter result;
                CompareTupleFilter newComp = new CompareTupleFilter(compf.getOperator());
                newComp.setNullString(nullString);
                newComp.addChild(new ColumnTupleFilter(col));
                String v;
                String firstValue = constValues.iterator().next();

                // translate constant into rowkey ID
                switch (newComp.getOperator()) {
                case EQ:
                case IN:
                    Set<String> newValues = Sets.newHashSet();
                    for (String value : constValues) {
                        v = translate(col, value, 0);
                        if (nullString.equals(v) == false)
                            newValues.add(v);
                    }
                    if (newValues.isEmpty()) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(newValues));
                        result = newComp;
                    }
                    break;
                case NEQ:
                    v = translate(col, firstValue, 0);
                    if (nullString.equals(v)) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(v));
                        result = newComp;
                    }
                    break;
                case LT:
                    v = translate(col, firstValue, 1);
                    if (nullString.equals(v)) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(v));
                        result = newComp;
                    }
                    break;
                case LTE:
                    v = translate(col, firstValue, -1);
                    if (nullString.equals(v)) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(v));
                        result = newComp;
                    }
                    break;
                case GT:
                    v = translate(col, firstValue, -1);
                    if (nullString.equals(v)) {
                        result = ConstantTupleFilter.TRUE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(v));
                        result = newComp;
                    }
                    break;
                case GTE:
                    v = translate(col, firstValue, 1);
                    if (nullString.equals(v)) {
                        result = ConstantTupleFilter.FALSE;
                    } else {
                        newComp.addChild(new ConstantTupleFilter(v));
                        result = newComp;
                    }
                    break;
                default:
                    throw new IllegalStateException("Cannot handle operator " + newComp.getOperator());
                }
                return result;
            }

            private String nullString(TblColRef column) {
                byte[] id = new byte[columnIO.getColumnLength(column)];
                for (int i = 0; i < id.length; i++) {
                    id[i] = Dictionary.NULL;
                }
                return Dictionary.dictIdToString(id, 0, id.length);
            }

            private String translate(TblColRef column, String v, int roundingFlag) {
                byte[] value = Bytes.toBytes(v);
                byte[] id = new byte[columnIO.getColumnLength(column)];
                columnIO.writeColumn(column, value, value.length, roundingFlag, Dictionary.NULL, id, 0);
                return Dictionary.dictIdToString(id, 0, id.length);
            }

        });
        TupleFilter copy = TupleFilterSerializer.deserialize(bytes);
        return new SRowFilter(copy);
    }

    public static byte[] serialize(SRowFilter o) {
        return (o.filter == null) ? BytesUtil.EMPTY_BYTE_ARRAY : TupleFilterSerializer.serialize(o.filter);
    }

    public static SRowFilter deserialize(byte[] filterBytes) {
        TupleFilter filter = (filterBytes == null || filterBytes.length == 0) //
        ? null //
                : TupleFilterSerializer.deserialize(filterBytes);
        return new SRowFilter(filter);
    }

    // ============================================================================

    protected final TupleFilter filter;

    protected SRowFilter(TupleFilter filter) {
        this.filter = filter;
    }

    public boolean evaluate(ITuple tuple) {
        if (filter == null)
            return true;
        else
            return filter.evaluate(tuple);
    }

}
