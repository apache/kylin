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
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

import java.nio.ByteBuffer;

public class ColumnCardinalityAnalysisDelegate
        extends AbstractColumnAnalysisDelegate<ColumnCardinalityAnalysisDelegate> {

    private static final HLLCSerializer HLLC_SERIALIZER = new HLLCSerializer(DataType.getType("hllc14"));

    private transient HLLCounter hllCounter = new HLLCounter(14);

    private boolean hasNullOrBlank = false;

    public ColumnCardinalityAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        if (StringUtils.isBlank(strValue)) {
            hasNullOrBlank = true;
            return;
        }

        hllCounter.add(strValue);
    }

    @Override
    public void encode() {
        final ByteBuffer out = ByteBuffer.allocate(hllCounter.maxLength());
        HLLC_SERIALIZER.serialize(hllCounter, out);
        this.buffer = out.array();
    }

    @Override
    public void decode() {
        final ByteBuffer in = ByteBuffer.wrap(this.buffer);
        hllCounter = HLLC_SERIALIZER.deserialize(in);
    }

    @Override
    protected ColumnCardinalityAnalysisDelegate doReduce(ColumnCardinalityAnalysisDelegate another) {
        this.hasNullOrBlank = (this.hasNullOrBlank || another.hasNullOrBlank);
        this.hllCounter.merge(another.hllCounter);
        return this;
    }

    public long getCardinality() {
        if (hasNullOrBlank) {
            return hllCounter.getCountEstimate() + 1L;
        }

        return hllCounter.getCountEstimate();
    }

    public HLLCounter getHllC() {
        return hllCounter;
    }
}
