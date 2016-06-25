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

package org.apache.kylin.rest.response;

import java.io.Serializable;

public class HBaseResponse implements Serializable {
    private static final long serialVersionUID = 7263557115683263492L;
    private String tableName;
    private long tableSize;
    private int regionCount;
    private long dateRangeStart;
    private long dateRangeEnd;

    public HBaseResponse() {
    }

    /**
     * @return The hbase table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName
     *            The hbase table name.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the tableSize
     */
    public long getTableSize() {
        return tableSize;
    }

    /**
     * @param tableSize
     *            the tableSize to set
     */
    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    /**
     * @return the regionCount
     */
    public int getRegionCount() {
        return regionCount;
    }

    /**
     * @param regionCount
     *            the regionCount to set
     */
    public void setRegionCount(int regionCount) {
        this.regionCount = regionCount;
    }

    /**
     * @return the segmentStartTime
     */
    public long getDateRangeStart() {
        return dateRangeStart;
    }

    /**
     * @param segmentStartTime
     *            the segmentStartTime to set
     */
    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    /**
     * @return the segmentEndTime
     */
    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    /**
     * @param segmentEndTime
     *            the segmentEndTime to set
     */
    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }
}
