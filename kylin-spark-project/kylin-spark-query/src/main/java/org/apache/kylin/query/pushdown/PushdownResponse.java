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

package org.apache.kylin.query.pushdown;

import org.apache.kylin.engine.spark.metadata.cube.StructField;

import java.util.List;

public class PushdownResponse {
    private List<StructField> columns;
    private List<List<String>> rows;

    public PushdownResponse(List<StructField> columns, List<List<String>> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    public List<StructField> getColumns() {
        return this.columns;
    }

    public List<List<String>> getRows() {
        return this.rows;
    }

    public void setColumns(List<StructField> columns) {
        this.columns = columns;
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }
}
