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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class CCRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("cc")
    private ComputedColumnDesc cc;

    public int[] genDependIds(NDataModel dataModel) {
        Preconditions.checkArgument(cc != null, "CCRecItemV2 without computed column object.");
        Preconditions.checkArgument(StringUtils.isNotEmpty(cc.getExpression()),
                "Computed column expression cannot be null.");
        val exprIdentifiers = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(cc.getExpression());
        int[] arr = new int[exprIdentifiers.size()];
        ImmutableBiMap<Integer, TblColRef> effectiveCols = dataModel.getEffectiveCols();
        Map<String, Integer> map = Maps.newHashMap();
        effectiveCols.forEach((k, v) -> map.put(v.getIdentity(), k));
        for (int i = 0; i < exprIdentifiers.size(); i++) {
            String columnName = exprIdentifiers.get(i).getFirst() + "." + exprIdentifiers.get(i).getSecond();
            Integer integer = map.get(columnName);
            Preconditions.checkArgument(integer != null, "Computed column referred to a column not on model.");
            arr[i] = integer;
        }
        return arr;
    }
}
