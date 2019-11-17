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
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

public class NumericalColumnAnalysisDelegate extends AbstractColumnAnalysisDelegate<NumericalColumnAnalysisDelegate> {

    private double maxValue = Double.NaN;

    private double minValue = Double.NaN;

    public NumericalColumnAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        if (StringUtils.isBlank(strValue)) {
            return;
        }

        if (!NumberUtils.isNumber(strValue)) {
            return;
        }

        final double value = NumberUtils.createDouble(strValue);
        if (Double.isNaN(maxValue) || value > maxValue) {
            maxValue = value;
        }

        if (Double.isNaN(minValue) || value < minValue) {
            minValue = value;
        }

    }

    @Override
    protected NumericalColumnAnalysisDelegate doReduce(NumericalColumnAnalysisDelegate another) {
        this.maxValue = this.getMax() > another.getMax() ? this.maxValue : another.maxValue;
        this.minValue = this.getMin() < another.getMin() ? this.minValue : another.minValue;

        return this;
    }

    public double getMax() {
        return maxValue;
    }

    public double getMin() {
        return minValue;
    }

}
