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
package org.apache.kylin.query.util;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;

public class ColumnRowTypeMockUtil {
    public static ColumnRowType mock(String tableName, String tableAlias,
            List<Pair<String, String>> columnNamesAndTypes) {
        TableDesc tableDesc = TableDesc.mockup(tableName);
        List<ColumnDesc> columnDescList = Lists.newArrayList();
        for (Pair<String, String> columnNamesAndType : columnNamesAndTypes) {
            columnDescList.add(
                    ColumnDesc.mockup(tableDesc, 0, columnNamesAndType.getFirst(), columnNamesAndType.getSecond()));
        }
        tableDesc.setColumns(columnDescList.toArray(new ColumnDesc[0]));
        final TableRef tableRef = TblColRef.tableForUnknownModel(tableAlias, tableDesc);

        List<TblColRef> tblColRefs = columnDescList.stream()
                .map(input -> TblColRef.columnForUnknownModel(tableRef, input)).collect(Collectors.toList());

        return new ColumnRowType(tblColRefs);
    }
}
