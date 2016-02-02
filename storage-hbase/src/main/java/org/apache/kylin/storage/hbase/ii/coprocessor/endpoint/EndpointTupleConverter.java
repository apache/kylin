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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import java.util.List;

import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;

public class EndpointTupleConverter {

    final TupleInfo tupleInfo;
    final List<TblColRef> columns;
    final int[] columnTupleIdx;
    final int[] aggrTupleIdx;

    public EndpointTupleConverter(List<TblColRef> columns, List<FunctionDesc> aggrMeasures, TupleInfo returnTupleInfo) {
        this.tupleInfo = returnTupleInfo;
        this.columns = columns;
        this.columnTupleIdx = new int[columns.size()];
        this.aggrTupleIdx = new int[aggrMeasures.size()];

        for (int i = 0; i < columns.size(); i++) {
            TblColRef col = columns.get(i);
            columnTupleIdx[i] = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
        }

        for (int i = 0; i < aggrMeasures.size(); i++) {
            FunctionDesc measure = aggrMeasures.get(i);
            int tupleIdx;
            // for dimension playing as metrics, the measure is just a placeholder, the real value comes from columns
            if (measure.isDimensionAsMetric()) {
                tupleIdx = -1;
            }
            // a rewrite metrics is identified by its rewrite field name
            else if (measure.needRewrite()) {
                String rewriteFieldName = measure.getRewriteFieldName();
                tupleIdx = tupleInfo.hasField(rewriteFieldName) ? tupleInfo.getFieldIndex(rewriteFieldName) : -1;
            }
            // a non-rewrite metrics (i.e. sum) is like a dimension column
            else {
                TblColRef col = measure.getParameter().getColRefs().get(0);
                tupleIdx = tupleInfo.hasColumn(col) ? tupleInfo.getColumnIndex(col) : -1;
            }
            aggrTupleIdx[i] = tupleIdx;
        }
    }

    public ITuple makeTuple(TableRecord tableRecord, List<Object> measureValues, Tuple tuple) {
        // dimensions and metrics from II table record 
        for (int i = 0; i < columnTupleIdx.length; i++) {
            int tupleIdx = columnTupleIdx[i];
            if (tupleIdx >= 0) {
                String value = tableRecord.getValueString(i);
                tuple.setDimensionValue(tupleIdx, value);
            }
        }

        // additional aggregations calculated inside end point (like cube measures)
        if (measureValues != null) {
            for (int i = 0; i < aggrTupleIdx.length; ++i) {
                int tupleIdx = aggrTupleIdx[i];
                if (tupleIdx >= 0) {
                    Object value = measureValues.get(i);
                    if (value instanceof String) {
                        String dataType = tuple.getDataTypeName(tupleIdx);
                        value = Tuple.convertOptiqCellValue((String) value, dataType);
                    }
                    tuple.setMeasureValue(tupleIdx, value);
                }
            }
        }
        return tuple;
    }

}
