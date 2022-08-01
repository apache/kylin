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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MeasureRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("measure")
    private NDataModel.Measure measure;
    @JsonProperty("param_order")
    private long[] paramOrder;

    public int[] genDependIds(Map<String, RawRecItem> nonLayoutUniqueFlagRecMap, String content, NDataModel dataModel) {
        Set<TableRef> allTables = dataModel.getAllTableRefs();
        Map<String, TableRef> tableMap = Maps.newHashMap();
        allTables.forEach(tableRef -> tableMap.putIfAbsent(tableRef.getAlias(), tableRef));
        Map<String, NDataModel.NamedColumn> namedColumnMap = getNamedColumnMap(dataModel);
        String[] params = content.split("__");
        int[] dependIDs = new int[params.length - 1];
        for (int i = 1; i < params.length; i++) {
            if (nonLayoutUniqueFlagRecMap.containsKey(params[i])) {
                // means it's a cc
                dependIDs[i - 1] = -1 * nonLayoutUniqueFlagRecMap.get(params[i]).getId();
            } else {

                String[] splits = params[i].split(RawRecUtil.TABLE_COLUMN_SEPARATOR);
                if (splits.length == 2) {
                    try {
                        String alias = splits[0];
                        Preconditions.checkArgument(tableMap.containsKey(alias));
                        TableDesc tableDesc = tableMap.get(alias).getTableDesc();
                        ColumnDesc dependColumn = RawRecUtil.findColumn(splits[1], tableDesc);
                        String aliasDotName = String.format(Locale.ROOT, "%s.%s", alias, dependColumn.getName());
                        dependIDs[i - 1] = namedColumnMap.get(aliasDotName).getId();
                    } catch (IllegalArgumentException e) {
                        dependIDs[i - 1] = Integer.MAX_VALUE;
                    }
                } else {
                    dependIDs[i - 1] = Integer.MAX_VALUE;
                }
            }
        }
        return dependIDs;
    }

    public static Map<String, NDataModel.NamedColumn> getNamedColumnMap(NDataModel dataModel) {
        Map<String, NDataModel.NamedColumn> namedColumnMap = Maps.newHashMap();
        dataModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist).forEach(namedColumn -> {
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            namedColumnMap.putIfAbsent(aliasDotColumn, namedColumn);
        });
        return namedColumnMap;
    }
}
