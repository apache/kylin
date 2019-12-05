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
