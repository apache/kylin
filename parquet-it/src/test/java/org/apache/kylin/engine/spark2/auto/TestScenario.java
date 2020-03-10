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

package org.apache.kylin.engine.spark2.auto;

import org.apache.kylin.common.util.Pair;
import java.util.List;
import java.util.Set;
import org.apache.kylin.engine.spark2.NExecAndComp.CompareLevel;


public class TestScenario {

        String folderName;
        private CompareLevel compareLevel;
        JoinType joinType;
        private int fromIndex;
        private int toIndex;
        private boolean isLimit;
        private Set<String> exclusionList;
        private boolean isDynamicSql = false;

        // value when execute
        List<Pair<String, String>> queries;

        TestScenario(String folderName) {
            this(CompareLevel.SAME, folderName);
        }

        TestScenario(String folderName, int fromIndex, int toIndex) {
            this(CompareLevel.SAME, folderName, fromIndex, toIndex);
        }

        TestScenario(CompareLevel compareLevel, String folder) {
            this(compareLevel, JoinType.DEFAULT, folder);
        }

        TestScenario(CompareLevel compareLevel, JoinType joinType, String folder) {
            this(compareLevel, joinType, false, folder, 0, 0, null);
        }

        public TestScenario(CompareLevel compareLevel, String folder, int fromIndex, int toIndex) {
            this(compareLevel, JoinType.DEFAULT, false, folder, fromIndex, toIndex, null);
        }

        TestScenario(CompareLevel compareLevel, String folder, Set<String> exclusionList) {
            this(compareLevel, JoinType.DEFAULT, false, folder, 0, 0, exclusionList);
        }

        TestScenario(CompareLevel compareLevel, boolean isLimit, String folder) {
            this(compareLevel, JoinType.DEFAULT, isLimit, folder, 0, 0, null);
        }

        public TestScenario(CompareLevel compareLevel, JoinType joinType, boolean isLimit, String folderName,
                int fromIndex, int toIndex, Set<String> exclusionList) {
            this.compareLevel = compareLevel;
            this.folderName = folderName;
            this.joinType = joinType;
            this.isLimit = isLimit;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.exclusionList = exclusionList;
        }

        public CompareLevel getCompareLevel() {
            return this.compareLevel;
        }

        public boolean isDynamicSql() {
            return this.isDynamicSql;
        }

        public List<Pair<String, String>> getQueries() {
            return this.queries;
        }

        public void setDynamicSql(boolean isDynamicSql) {
            this.isDynamicSql = isDynamicSql;
        }
    } // end TestScenario

    enum JoinType {

        /**
         * Left outer join.
         */
        LEFT,

        /**
         * Inner join
         */
        INNER,

        /**
         * original state
         */
        DEFAULT
    }


