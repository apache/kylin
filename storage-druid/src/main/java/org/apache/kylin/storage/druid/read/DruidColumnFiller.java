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

package org.apache.kylin.storage.druid.read;

import java.util.Objects;

import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.druid.DruidSchema;

public class DruidColumnFiller implements ColumnFiller {
    private final DruidSchema schema;

    private final int[] tupleIndex;
    private final MeasureType[] measureTypes;

    public DruidColumnFiller(DruidSchema schema, TupleInfo tupleInfo) {
        this.schema = schema;
        this.tupleIndex = new int[schema.getTotalFieldCount()];
        this.measureTypes = new MeasureType[schema.getTotalFieldCount()];

        int i = 0;
        for (TblColRef dim : schema.getDimensions()) {
            tupleIndex[i++] = tupleInfo.getColumnIndex(dim);
        }
        for (MeasureDesc met : schema.getMeasures()) {
            FunctionDesc func = met.getFunction();
            MeasureType<?> measureType = func.getMeasureType();

            if (func.needRewrite()) {
                String fieldName = func.getRewriteFieldName();
                tupleIndex[i] = tupleInfo.getFieldIndex(fieldName);
            } else {
                TblColRef col = func.getParameter().getColRefs().get(0);
                tupleIndex[i] = tupleInfo.getColumnIndex(col);
            }

            if (!measureType.needAdvancedTupleFilling()) {
                measureTypes[i] = measureType;
            } else if (measureType instanceof ExtendedColumnMeasureType) {
                final TblColRef extended = ExtendedColumnMeasureType.getExtendedColumn(func);
                final int extendedColumnInTupleIdx = tupleInfo.hasColumn(extended) ? tupleInfo.getColumnIndex(extended) : -1;
                tupleIndex[i] = extendedColumnInTupleIdx;
            } else {
                throw new UnsupportedOperationException("Unsupported measure type : " + measureType);
            }

            i++;
        }
    }

    public void fill(Object[] row, Tuple tuple) {
        for (int i = 0; i < schema.getDimensions().size(); i++) {
            tuple.setDimensionValue(tupleIndex[i], Objects.toString(row[i], null));
        }

        for (int i = schema.getDimensions().size(); i < schema.getTotalFieldCount(); i++) {
            if (measureTypes[i] != null) {
                measureTypes[i].fillTupleSimply(tuple, tupleIndex[i], row[i]);
            } else {
                //for ExtendedColumn
                tuple.setDimensionValue(tupleIndex[i], (String) row[i]);
            }
        }
    }
}
