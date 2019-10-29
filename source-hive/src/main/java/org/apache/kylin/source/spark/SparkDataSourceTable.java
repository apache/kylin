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

package org.apache.kylin.source.spark;

import java.io.IOException;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;


/**
 * IReadableTable implementation for reading the spark datasource table.
 */
public class SparkDataSourceTable implements IReadableTable {
    private TableDesc tableDesc;

    public SparkDataSourceTable(TableDesc tableDesc) {
        if (null == tableDesc) {
            throw new RuntimeException("Table desc to init spark datasource can not be null");
        }

        this.tableDesc = tableDesc;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new SparkDataSourceTableReader(tableDesc);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        TableSignature signature = new TableSignature();
        return signature;
    }

    @Override
    public boolean exists() throws IOException {
        return false;
    }

    @Override
    public String toString() {
        return "spark datasource: tableType = " + tableDesc.getTableType()
                + ", table=[" + tableDesc.getName() + "]";
    }
}
