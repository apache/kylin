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

package org.apache.kylin.jdbc;

class KylinCheckSQL {
    public static int countDynamicPlaceholder(String sql) {
        int placeholderCount = 0;
        boolean singleQuotes = true;
        boolean doubleQuotes = true;
        boolean backticks = true;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '\'') {
                singleQuotes = !singleQuotes;
            } else if (sql.charAt(i) == '\"') {
                doubleQuotes = !doubleQuotes;
            } else if (sql.charAt(i) == '`') {
                backticks = !backticks;
            } else if (sql.charAt(i) == '?') {
                if (singleQuotes && doubleQuotes && backticks) {
                    placeholderCount++;
                }
            }
        }
        return placeholderCount;
    }
}
