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
