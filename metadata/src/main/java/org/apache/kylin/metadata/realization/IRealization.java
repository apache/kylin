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

package org.apache.kylin.metadata.realization;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.List;

public interface IRealization {

    public boolean isCapable(SQLDigest digest);

    /**
     * Given the features of a query, return an integer indicating how capable the realization
     * is to answer the query.
     *
     * @return -1 if the realization cannot fulfill the query;
     * or a number between 0-100 if the realization can answer the query, the smaller
     * the number, the more efficient the realization.
     * Especially,
     * 0 - means the realization has the exact result pre-calculated, no less no more;
     * 100 - means the realization will scan the full table with little or no indexing.
     */
    public int getCost(SQLDigest digest);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     *
     * @return
     */
    public RealizationType getType();

    public DataModelDesc getDataModelDesc();

    public String getFactTable();

    public List<TblColRef> getAllColumns();

    public List<TblColRef> getAllDimensions();

    public List<MeasureDesc> getMeasures();

    public boolean isReady();

    public String getName();

    public String getCanonicalName();

    public String getProjectName();

    public void setProjectName(String prjName);

    public long getDateRangeStart();

    public long getDateRangeEnd();

    public String getModelName();
}
