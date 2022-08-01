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

package org.apache.kylin.metadata.recommendation.entity;

import java.io.Serializable;
import java.util.Map;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DimensionRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("column")
    private NDataModel.NamedColumn column;
    @JsonProperty("data_type")
    private String dataType;

    public int[] genDependIds(Map<String, RawRecItem> uniqueRecItemMap, String content, NDataModel dataModel) {
        if (uniqueRecItemMap.containsKey(content)) {
            return new int[] { -1 * uniqueRecItemMap.get(content).getId() };
        } else {
            String[] arr = content.split(RawRecUtil.TABLE_COLUMN_SEPARATOR);
            if (arr.length == 2) {
                try {
                    Map<String, TableRef> tableAliasMap = dataModel.getAliasMap();
                    Preconditions.checkArgument(tableAliasMap.containsKey(arr[0]));
                    TableRef tableRef = tableAliasMap.get(arr[0]);
                    ColumnDesc tableColumn = RawRecUtil.findColumn(arr[1], tableRef.getTableDesc());
                    String columnName = column.getAliasDotColumn().split("\\.")[1];
                    Preconditions.checkArgument(tableColumn.getName().equalsIgnoreCase(columnName));
                } catch (Exception e) {
                    log.error("validate DimensionRecItemV2 dependIds error.", e);
                    return new int[] { Integer.MAX_VALUE };
                }
            }
            return new int[] { getColumn().getId() };
        }
    }
}
