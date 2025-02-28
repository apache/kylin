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

package org.apache.kylin.engine.spark.source;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSparkTable implements IReadableTable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTable.class);

    private final String database;
    private final String tableName;

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
        String path = String.format(Locale.ROOT, "%s.%s", database, tableName);
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
