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

package org.apache.kylin.rec.query;

import java.util.Map;

import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.apache.kylin.rec.query.validator.SQLValidateResult;

class SqlValidateTestBase extends AutoTestOnLearnKylinData {
    void printSqlValidateResults(Map<String, SQLValidateResult> validateStatsMap) {
        validateStatsMap.forEach((key, sqlValidateResult) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("sql: ").append(key).append(",\n\t");
            sb.append("capable: ").append(sqlValidateResult.isCapable()).append(",\n\t");

            sqlValidateResult.getSqlAdvices().forEach(sqlAdvice -> {
                sb.append("reason:").append(sqlAdvice.getIncapableReason()).append("\n\t");
                sb.append("suggest:").append(sqlAdvice.getSuggestion()).append("\n\t");
            });

            SQLResult sqlResult = sqlValidateResult.getResult();
            sb.append("project: ").append(sqlResult.getProject()).append("\n\t");
            sb.append("duration: ").append(sqlResult.getDuration()).append("\n\t");
            sb.append("query id: ").append(sqlResult.getQueryId()).append("\n\t");
            System.out.println(sb);
        });
    }
}
