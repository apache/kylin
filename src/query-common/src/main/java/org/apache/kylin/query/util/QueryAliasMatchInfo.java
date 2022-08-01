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

import java.util.LinkedHashMap;

import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.alias.AliasMapping;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class QueryAliasMatchInfo extends AliasMapping {
    // each alias's ColumnRowType
    private LinkedHashMap<String, ColumnRowType> alias2CRT;

    // for model view
    private NDataModel model;

    public QueryAliasMatchInfo(BiMap<String, String> aliasMapping, LinkedHashMap<String, ColumnRowType> alias2CRT) {
        super(aliasMapping);
        this.alias2CRT = alias2CRT;
    }

    private QueryAliasMatchInfo(BiMap<String, String> aliasMapping, NDataModel model) {
        super(aliasMapping);
        this.model = model;
    }

    public static QueryAliasMatchInfo fromModelView(String queryTableAlias, NDataModel model) {
        BiMap<String, String> map = HashBiMap.create();
        map.put(queryTableAlias, model.getAlias());
        return new QueryAliasMatchInfo(map, model);
    }

    LinkedHashMap<String, ColumnRowType> getAlias2CRT() {
        return alias2CRT;
    }

    public boolean isModelView() {
        return model != null;
    }

    public NDataModel getModel() {
        return model;
    }

}
