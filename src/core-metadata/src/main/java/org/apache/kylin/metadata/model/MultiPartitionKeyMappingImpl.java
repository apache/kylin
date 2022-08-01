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

package org.apache.kylin.metadata.model;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

@Getter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MultiPartitionKeyMappingImpl extends MultiPartitionKeyMapping {

    private transient List<TblColRef> aliasColumnRefs;
    private transient ImmutableMultimap<List<String>, List<String>> immutableValueMapping;

    public MultiPartitionKeyMappingImpl(List<String> multiPartitionCols, List<String> aliasCols,
            List<Pair<List<String>, List<String>>> valueMapping) {
        setAliasCols(aliasCols);
        setMultiPartitionCols(multiPartitionCols);
        setValueMapping(valueMapping);
    }

    public void init(NDataModel model) {
        if (CollectionUtils.isEmpty(getAliasCols()) || CollectionUtils.isEmpty(getMultiPartitionCols())) {
            return;
        }

        aliasColumnRefs = Lists.newArrayList();
        for (String columnName : getAliasCols()) {
            aliasColumnRefs.add(model.findColumn(columnName));
        }

        ImmutableMultimap.Builder<List<String>, List<String>> builder = ImmutableMultimap.builder();
        getValueMapping().forEach(pair -> {
            val partitionValue = pair.getFirst();
            val aliasValue = pair.getSecond();

            builder.put(partitionValue, aliasValue);
        });
        immutableValueMapping = builder.build();
    }

    public List<TblColRef> getAliasColumns() {
        return aliasColumnRefs;
    }

    public Collection<List<String>> getAliasValue(List<String> partitionValues) {
        return immutableValueMapping.get(partitionValues);
    }
}
