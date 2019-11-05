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

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public abstract class AbstractColumnAnalysisDelegate<T extends AbstractColumnAnalysisDelegate> implements Serializable {

    protected final ColumnDesc columnDesc;

    protected byte[] buffer;

    public AbstractColumnAnalysisDelegate(ColumnDesc columnDesc) {
        this.columnDesc = columnDesc;
    }

    public abstract void analyze(Row row, Object colValue);

    public T reduce(final T another) {
        if (!this.getClass().equals(another.getClass())) {
            throw new IllegalStateException();
        }

        if (!this.columnDesc.equals(another.columnDesc)) {
            throw new IllegalStateException();
        }

        return doReduce(another);
    }

    protected abstract T doReduce(T another);

    public void encode() {

    }

    public void decode() {

    }

    public ColumnDesc getColumnDesc() {
        return columnDesc;
    }

    public byte[] getBuffer() {
        return buffer;
    }
}
