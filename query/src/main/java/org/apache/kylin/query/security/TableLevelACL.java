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

package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

public class TableLevelACL {
    public static void tableFilter(List<OLAPContext> contexts, List<String> tableBlackList) {
        Set<String> tableWithSchema = getTableWithSchema(contexts);
        for (String tbl : tableBlackList) {
            if (tableWithSchema.contains(tbl.toUpperCase(Locale.ROOT))) {
                //                throw new kylin.AccessDeniedException("table:" + tbl);
                System.out.println("Access table:" + tbl + " denied");
            }
        }
    }

    public static void columnFilter(List<OLAPContext> contexts, List<String> columnBlackList) {
        List<String> allColWithTblAndSchema = getAllColWithTblAndSchema(contexts);
        for (String tbl : columnBlackList) {
            if (allColWithTblAndSchema.contains(tbl.toUpperCase(Locale.ROOT))) {
                //                throw new kylin.AccessDeniedException("table:" + tbl);
                System.out.println("Access table:" + tbl + " denied");
            }
        }
    }

    public static List<String> getAllColWithTblAndSchema(List<OLAPContext> contexts) {
        // all columns with table and DB. Like DB.TABLE.COLUMN
        List<String> allColWithTblAndSchema = new ArrayList<>();
        for (OLAPContext context : contexts) {
            Set<TblColRef> allColumns = context.allColumns;
            for (TblColRef tblColRef : allColumns) {
                allColWithTblAndSchema.add(tblColRef.getColumWithTableAndSchema());
            }
        }
        return allColWithTblAndSchema;
    }

    public static Set<String> getTableWithSchema(List<OLAPContext> contexts) {
        // all tables with DB, Like DB.TABLE
        Set<String> tableWithSchema = new HashSet<>();
        for (OLAPContext context : contexts) {
            Set<TblColRef> allColumns = context.allColumns;
            for (TblColRef tblColRef : allColumns) {
                tableWithSchema.add(tblColRef.getTableWithSchema());
            }
        }
        return tableWithSchema;
    }

    public static List<String> mockTableBlackList() {
        List<String> blackList = new ArrayList<>();
        blackList.add("DEFAULT.KYLIN_SALES");
        return blackList;
    }
}
