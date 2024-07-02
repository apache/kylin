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

package org.apache.kylin.metadata.recommendation.util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.springframework.util.DigestUtils;

public class RawRecUtil {

    public static final String TABLE_COLUMN_SEPARATOR = "\\$";
    public static final String MEASURE_PARAM_SEPARATOR = "__";

    private RawRecUtil() {
    }

    public static ColumnDesc findColumn(String idOrColumnName, @Nonnull TableDesc tableDesc) {
        ColumnDesc[] columns = tableDesc.getColumns();
        ColumnDesc dependColumn = tableDesc.findColumnByName(idOrColumnName);
        if (dependColumn == null) {
            // compatible to old version
            dependColumn = columns[Integer.parseInt(idOrColumnName)];
        }
        return dependColumn;
    }

    public static String dimensionUniqueContent(TblColRef tblColRef, Map<String, ComputedColumnDesc> ccMap,
            Set<String> newCcUuids) {
        return colUniqueName(tblColRef, ccMap, newCcUuids);
    }

    public static String measureUniqueContent(NDataModel.Measure measure, Map<String, ComputedColumnDesc> ccMap,
            Set<String> newCcUuids) {
        Set<String> paramNames = Sets.newHashSet();
        List<ParameterDesc> parameters = measure.getFunction().getParameters();
        parameters.forEach(param -> {
            TblColRef colRef = param.getColRef();
            if (colRef == null) {
                paramNames.add(String.valueOf(Integer.MAX_VALUE));
                return;
            }
            paramNames.add(colUniqueName(colRef, ccMap, newCcUuids));
        });
        return String.format(Locale.ROOT, "%s__%s", measure.getFunction().getExpression(),
                String.join("__", paramNames));
    }

    private static String colUniqueName(TblColRef tblColRef, Map<String, ComputedColumnDesc> ccMap,
            Set<String> newCcUuids) {
        final ColumnDesc columnDesc = tblColRef.getColumnDesc();
        String uniqueName;
        if (columnDesc.isComputedColumn()) {
            /* if cc is new, unique_name forward to its uuid,
             * otherwise table_alias.column
             */
            ComputedColumnDesc cc = ccMap.get(columnDesc.getIdentity());
            if (newCcUuids.contains(cc.getUuid())) {
                uniqueName = cc.getUuid();
            } else {
                uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getName();
            }
        } else {
            uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getName();
        }
        return uniqueName;
    }

    public static ComputedColumnDesc getCC(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.COMPUTED_COLUMN == rawRecItem.getType());
        CCRecItemV2 recItemV2 = (CCRecItemV2) rawRecItem.getRecEntity();
        return recItemV2.getCc();
    }

    public static NDataModel.Measure getMeasure(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.MEASURE == rawRecItem.getType());
        MeasureRecItemV2 recItemV2 = (MeasureRecItemV2) rawRecItem.getRecEntity();
        return recItemV2.getMeasure();
    }

    private static LayoutRecItemV2 getLayoutRecItemV2(RawRecItem rawRecItem) {
        Preconditions.checkArgument(rawRecItem != null && rawRecItem.isLayoutRec());
        return (LayoutRecItemV2) rawRecItem.getRecEntity();
    }

    public static LayoutEntity getLayout(RawRecItem rawRecItem) {
        return getLayoutRecItemV2(rawRecItem).getLayout();
    }

    public static String getContent(RawRecItem recItem) {
        return getContent(recItem.getProject(), recItem.getModelID(), recItem.getRecEntity().getUniqueContent(),
                recItem.getType());
    }

    public static String getContent(String project, String model, String uniqueContent, RawRecItem.RawRecType type) {
        StringBuilder sb = new StringBuilder();
        sb.append(project).append(',');
        sb.append(model).append(',');
        sb.append(uniqueContent).append(',');
        sb.append(type);
        return sb.toString();
    }

    public static String computeMD5(String content) {
        return DigestUtils.md5DigestAsHex(content.getBytes(Charset.defaultCharset()));
    }

    public static Map<String, List<String>> uniqueFlagsToMd5Map(Set<String> uniqueFlags) {
        Map<String, List<String>> md5ToFlags = Maps.newHashMap();
        uniqueFlags.forEach(uniqueFlag -> {
            if (uniqueFlag != null) {
                String md5 = uniqueFlag.substring(0, 32);
                List<String> flags = md5ToFlags.getOrDefault(md5, new ArrayList<>());
                flags.add(uniqueFlag);
                md5ToFlags.put(md5, flags);
            }
        });
        return md5ToFlags;
    }

    public static Pair<String, RawRecItem> getRawRecItemFromMap(String md5, String content,
            Map<String, List<String>> md5ToFlags, Map<String, RawRecItem> uniqueRecItemMap) {
        List<String> flags = md5ToFlags.getOrDefault(md5, new ArrayList<>());
        int maxItemId = 0;
        for (String flag : flags) {
            RawRecItem item = uniqueRecItemMap.get(flag);
            if (RawRecUtil.getContent(item).equals(content)) {
                return Pair.newPair(flag, item);
            }
            maxItemId = Math.max(item.getId(), maxItemId);
        }
        if (!flags.contains(md5)) {
            return Pair.newPair(md5, null);
        } else {
            String uniqueFlag = String.format(Locale.ROOT, "%s_%d", md5, maxItemId);
            return Pair.newPair(uniqueFlag, null);
        }
    }
}
