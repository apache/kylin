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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.HashBiMap;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.alias.AliasMapping;
import org.apache.kylin.query.relnode.ColumnRowType;

import lombok.Getter;

@Getter
public class QueryAliasMatchInfo extends AliasMapping {

    private final NDataModel model;
    private final LinkedHashMap<String, ColumnRowType> alias2CRT; // each alias's ColumnRowType
    private boolean isModelView;

    public QueryAliasMatchInfo(BiMap<String, String> aliasMapping, LinkedHashMap<String, ColumnRowType> alias2CRT) {
        this(aliasMapping, alias2CRT, null, Collections.emptySet());
    }

    public QueryAliasMatchInfo(BiMap<String, String> aliasMapping, LinkedHashMap<String, ColumnRowType> alias2CRT,
            NDataModel model, Set<String> excludedColumns) {
        super(aliasMapping);
        this.alias2CRT = alias2CRT;
        this.model = model;
        this.getExcludedColumns().addAll(excludedColumns);
    }

    public static QueryAliasMatchInfo fromModelView(String queryTableAlias, NDataModel model,
            Set<String> excludedColumns) {
        BiMap<String, String> map = HashBiMap.create();
        map.put(queryTableAlias, model.getAlias());
        QueryAliasMatchInfo matchInfo = new QueryAliasMatchInfo(map, null, model, excludedColumns);
        matchInfo.isModelView = true;
        return matchInfo;
    }
}
