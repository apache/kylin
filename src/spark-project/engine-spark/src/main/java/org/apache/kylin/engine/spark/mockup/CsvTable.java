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
package org.apache.kylin.engine.spark.mockup;

import java.io.IOException;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

public class CsvTable implements IReadableTable {

    private String baseDir;
    private TableDesc tableDesc;

    public CsvTable(String baseDir, TableDesc tableDesc) {
        this.baseDir = baseDir;
        this.tableDesc = tableDesc;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new CsvTableReader(baseDir, tableDesc);
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return new TableSignature();
    }

    @Override
    public boolean exists() throws IOException {
        return true;
    }
}
