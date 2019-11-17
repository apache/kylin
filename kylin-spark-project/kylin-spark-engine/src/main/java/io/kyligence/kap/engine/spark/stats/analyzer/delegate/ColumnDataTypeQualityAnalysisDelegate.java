/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
