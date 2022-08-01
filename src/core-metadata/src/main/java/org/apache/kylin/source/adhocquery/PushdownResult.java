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

package org.apache.kylin.source.adhocquery;

import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;

public class PushdownResult {
    private final Iterable<List<String>> rows;
    private final int size;
    private final List<SelectedColumnMeta> columnMetas;

    public PushdownResult(Iterable<List<String>> rows, int size, List<SelectedColumnMeta> columnMetas) {
        this.rows = rows;
        this.size = size;
        this.columnMetas = columnMetas;
    }

    public static PushdownResult emptyResult() {
        return new PushdownResult(new LinkedList<>(), 0, new LinkedList<>());
    }

    public Iterable<List<String>> getRows() {
        return rows;
    }

    public int getSize() {
        return size;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        return columnMetas;
    }
}
