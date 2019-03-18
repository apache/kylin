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

package org.apache.kylin.source.hive;

import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRHiveDictUtil {
    private static final Logger logger = LoggerFactory.getLogger(MRHiveDictUtil.class);

    public enum DictHiveType {
        GroupBy("group_by"), MrDictLockPath("/mr_dict_lock/");
        private String name;

        DictHiveType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static String generateDropTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);
        ddl.append("DROP TABLE IF EXISTS " + table + ";").append(" \n");
        return ddl.toString();
    }

    public static String generateCreateTableStatement(IJoinedFlatTableDesc flatDesc) {
        StringBuilder ddl = new StringBuilder();
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);

        ddl.append("CREATE TABLE IF NOT EXISTS " + table + " \n");
        ddl.append("( \n ");
        ddl.append("dict_key" + " " + "STRING" + " COMMENT '' \n");
        ddl.append(") \n");
        ddl.append("COMMENT '' \n");
        ddl.append("PARTITIONED BY (dict_column string) \n");
        ddl.append("STORED AS SEQUENCEFILE \n");
        ddl.append(";").append("\n");
        return ddl.toString();
    }

    public static String generateInsertDataStatement(IJoinedFlatTableDesc flatDesc, String dictColumn) {
        String table = getHiveTableName(flatDesc, DictHiveType.GroupBy);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");

        int index = 0;
        for (TblColRef tblColRef : flatDesc.getAllColumns()) {
            if (JoinedFlatTable.colName(tblColRef, flatDesc.useAlias()).equalsIgnoreCase(dictColumn)) {
                break;
            }
            index++;
        }

        if (index == flatDesc.getAllColumns().size()) {
            // dictColumn not in flatDesc,need throw Exception
            index = -1;
        }

        TblColRef col = flatDesc.getAllColumns().get(index);
        sql.append(JoinedFlatTable.colName(col) + " \n");

        MRHiveDictUtil.appendJoinStatement(flatDesc, sql);

        //group by
        sql.append("GROUP BY ");
        sql.append(JoinedFlatTable.colName(col) + " \n");

        return "INSERT OVERWRITE TABLE " + table + " \n"
                + "PARTITION (dict_column = '" + dictColumn + "')" + " \n"
                + sql + ";\n";
    }


    public static String getHiveTableName(IJoinedFlatTableDesc flatDesc, DictHiveType dictHiveType) {
        StringBuffer table = new StringBuffer(flatDesc.getTableName());
        table.append("__");
        table.append(dictHiveType.getName());
        return table.toString();
    }

    public static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql) {
        sql.append("FROM " + flatDesc.getTableName() + "\n");
    }

}
