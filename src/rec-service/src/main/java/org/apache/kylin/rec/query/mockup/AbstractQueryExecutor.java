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

package org.apache.kylin.rec.query.mockup;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rec.query.QueryRecord;

public abstract class AbstractQueryExecutor {

    private static final ThreadLocal<QueryRecord> CURRENT_RECORD = new ThreadLocal<>();

    static QueryRecord getCurrentRecord() {
        QueryRecord record = CURRENT_RECORD.get();
        if (record == null) {
            record = new QueryRecord();
            CURRENT_RECORD.set(record);
        }
        return record;
    }

    static void clearCurrentRecord() {
        CURRENT_RECORD.remove();
    }

    /**
     * Execute the given SQL statement under a certain project name,
     * which returns a <code>QueryRecord</code> object.
     */
    public abstract QueryRecord execute(String project, KylinConfig kylinConfig, String sql);
}
