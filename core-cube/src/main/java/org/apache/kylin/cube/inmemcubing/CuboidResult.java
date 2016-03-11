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

package org.apache.kylin.cube.inmemcubing;

import org.apache.kylin.gridtable.GridTable;

/**
 */
public class CuboidResult {

    public long cuboidId;
    public GridTable table;
    public int nRows;
    public long timeSpent;
    public int aggrCacheMB;

    public CuboidResult(long cuboidId, GridTable table, int nRows, long timeSpent, int aggrCacheMB) {
        this.cuboidId = cuboidId;
        this.table = table;
        this.nRows = nRows;
        this.timeSpent = timeSpent;
        this.aggrCacheMB = aggrCacheMB;
    }

}
