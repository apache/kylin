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

package org.apache.kylin.dict;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;

/**
 * To build dictionary, we need a list of distinct values on a column.
 * For column on lookup table, simply scan the whole table since the table is small.
 * For column on fact table, the fact table is too big to iterate. So the build
 * engine will first extract distinct values (by a MR job for example), and
 * implement this interface to provide the result to DictionaryManager.
 */
public interface DistinctColumnValuesProvider {

    /** Return a ReadableTable contains only one column, each row being a distinct value. */
    public ReadableTable getDistinctValuesFor(TblColRef col);
}
