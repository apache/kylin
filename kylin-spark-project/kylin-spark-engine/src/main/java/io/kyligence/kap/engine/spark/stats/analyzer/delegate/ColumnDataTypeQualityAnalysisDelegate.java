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

package io.kyligence.kap.engine.spark.stats.analyzer.delegate;

import io.kyligence.kap.engine.spark.stats.utils.DateTimeCheckUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

public class ColumnDataTypeQualityAnalysisDelegate
        extends AbstractColumnAnalysisDelegate<ColumnDataTypeQualityAnalysisDelegate> {

    private final DataType dataType;

    private long illegalValueCounter;

    private boolean unknownType = false;

    public ColumnDataTypeQualityAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
        this.dataType = columnDesc.getType();
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        if (dataType.isDateTimeFamily()) {
            if ((!DateTimeCheckUtils.isDate(strValue) || !DateTimeCheckUtils.isTime(strValue))) {
                illegalValueCounter++;
            }

        } else if (dataType.isNumberFamily()) {
            if ((!NumberUtils.isNumber(strValue))) {
                illegalValueCounter++;
            }

        } else if (dataType.isStringFamily()) {
            if ((StringUtils.isNotBlank(strValue) && strValue.length() > dataType.getPrecision())) {
                illegalValueCounter++;
            }

        } else if (dataType.isBoolean()) {
            if (StringUtils.isBlank(strValue)
                    || (!"true".equalsIgnoreCase(strValue) && !"false".equalsIgnoreCase(strValue))) {
                illegalValueCounter++;
            }

        } else {
            unknownType = true;
        }
    }

    @Override
    protected ColumnDataTypeQualityAnalysisDelegate doReduce(ColumnDataTypeQualityAnalysisDelegate another) {
        this.illegalValueCounter += another.illegalValueCounter;
        this.unknownType = (this.unknownType || another.unknownType);
        return this;
    }

    public long getIllegalValueCount() {
        return illegalValueCounter;
    }

    public boolean isUnknownType() {
        return unknownType;
    }
}
