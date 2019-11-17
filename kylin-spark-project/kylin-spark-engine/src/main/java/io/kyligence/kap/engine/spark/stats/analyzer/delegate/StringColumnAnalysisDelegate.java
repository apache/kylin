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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

public class StringColumnAnalysisDelegate extends AbstractColumnAnalysisDelegate<StringColumnAnalysisDelegate> {

    private long nullOrBlankCounter;

    private int maxLength = Integer.MIN_VALUE;

    private String maxLengthValue;

    private int minLength = Integer.MAX_VALUE;

    private String minLengthValue;

    public StringColumnAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    protected StringColumnAnalysisDelegate doReduce(StringColumnAnalysisDelegate another) {

        this.nullOrBlankCounter += another.nullOrBlankCounter;

        this.maxLength = this.getMaxLength() > another.getMaxLength() ? this.maxLength : another.maxLength;
        this.maxLengthValue = this.getMaxLength() > another.getMaxLength() ? this.maxLengthValue
                : another.maxLengthValue;

        this.minLength = this.getMinLength() < another.getMinLength() ? this.minLength : another.minLength;
        this.minLengthValue = this.getMinLength() < another.getMinLength() ? this.minLengthValue
                : another.minLengthValue;

        return this;
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        final int colValueLength;
        if (StringUtils.isBlank(strValue)) {
            nullOrBlankCounter++;
            colValueLength = 0;
        } else {
            colValueLength = StringUtil.utf8Length(strValue);
        }

        if (colValueLength > maxLength) {
            maxLength = colValueLength;
            maxLengthValue = strValue;
        }

        if (colValueLength < minLength) {
            minLength = colValueLength;
            minLengthValue = strValue;
        }
    }

    public long getNullOrBlankCount() {
        return nullOrBlankCounter;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String getMaxLengthValue() {
        return maxLengthValue == null ? "" : maxLengthValue;
    }

    public int getMinLength() {
        return minLength;
    }

    public String getMinLengthValue() {
        return minLengthValue == null ? "" : minLengthValue;
    }

}
