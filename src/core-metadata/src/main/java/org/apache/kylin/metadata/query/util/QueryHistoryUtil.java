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

package org.apache.kylin.metadata.query.util;

import java.sql.JDBCType;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistorySql;

import com.fasterxml.jackson.core.JsonProcessingException;

public class QueryHistoryUtil {

    private QueryHistoryUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static String getDownloadData(QueryHistory queryHistory, ZoneOffset zoneOffset, int zoneOffsetOfHours) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(zoneOffset));
        String sign = zoneOffsetOfHours > 0 ? "+" : "";
        String formatQueryTime = simpleDateFormat.format(queryHistory.getQueryTime()) + " GMT" + sign
                + zoneOffsetOfHours;

        String answerBy;
        if (queryHistory.getNativeQueryRealizations() != null && !queryHistory.getNativeQueryRealizations().isEmpty()) {
            answerBy = "\"[" + StringUtils.join(queryHistory.getNativeQueryRealizations().stream()
                    .map(NativeQueryRealization::getModelAlias).collect(Collectors.toList()), ',') + "]\"";
        } else {
            answerBy = queryHistory.getEngineType();
        }
        String queryMsg = queryHistory.getQueryHistoryInfo().getQueryMsg();
        if (StringUtils.isNotEmpty(queryMsg)) {
            queryMsg = "\"" + queryMsg.replace("\"", "\"\"") + "\"";
        }

        QueryHistorySql queryHistorySql = queryHistory.getQueryHistorySql();
        String sql = queryHistorySql.getNormalizedSql();

        return StringUtils
                .join(Lists.newArrayList(formatQueryTime, queryHistory.getDuration() + "ms", queryHistory.getQueryId(),
                        "\"" + sql.replace("\"", "\"\"") + "\"", answerBy, queryHistory.getQueryStatus(),
                        queryHistory.getHostName(), queryHistory.getQuerySubmitter(), queryMsg), ',')
                .replaceAll("\n|\r", " ");
    }

    public static String toQueryHistorySqlText(QueryHistorySql queryHistorySql) throws JsonProcessingException {
        return JsonUtil.writeValueAsString(queryHistorySql);
    }

    public static String toDataType(String className) throws ClassNotFoundException {
        ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(Class.forName(className));
        return JDBCType.valueOf(rep.typeId).getName();
    }
}
