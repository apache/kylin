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

package io.kyligence.kap.engine.spark.source;

import java.io.IOException;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSparkTable implements IReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTable.class);

    final private String database;
    final private String tableName;

    public NSparkTable(TableDesc tableDesc) {
        this.database = tableDesc.getDatabase();
        this.tableName = tableDesc.getName();
    }

    @Override
    public TableReader getReader() throws IOException {
        return new NSparkTableReader(database, tableName);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        // TODO: 07/12/2017 get modify time
        String path = String.format("%s.%s", database, tableName);
        long lastModified = System.currentTimeMillis(); // assume table is ever changing
        int size = 0;
        return new TableSignature(path, size, lastModified);
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public String toString() {
        return "spark: database=[" + database + "], table=[" + tableName + "]";
    }

}
