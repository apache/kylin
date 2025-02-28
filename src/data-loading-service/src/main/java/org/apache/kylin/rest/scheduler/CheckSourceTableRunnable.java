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

package org.apache.kylin.rest.scheduler;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CheckSourceTableRunnable extends AbstractSchedulerRunnable {

    private static final String SNAPSHOT_TABLE_CHECK_ERROR_MESSAGE = "Project[%s] Snapshot source table[%s] check table stats Failed";

    @Override
    public void execute() {
        checkTable();
    }

    public void checkTable() {
        try {
            val split = tableIdentity.split("\\.");
            val url = String.format(Locale.ROOT, "http://%s/kylin/api/snapshots/source_table_stats",
                    config.getServerAddress());
            val req = Maps.newHashMap();
            req.put("project", project);
            val length = split.length;
            int databaseStartIndex = 0;
            if (length > 2) {
                //Include catalog name
                req.put("catalog", split[0]);
                databaseStartIndex = 1;

            }
            val databaseBuffer = new StringBuilder();
            for (int i = databaseStartIndex; i < length - 1; i++) {
                databaseBuffer.append(".").append(split[i]);
            }
            req.put("database", databaseBuffer.substring(1));
            req.put("table", split[length - 1]);
            req.put("snapshot_partition_col", partitionColumn);
            log.debug("checkTableNeedRefresh request: {}", req);
            val httpHeaders = new HttpHeaders();
            httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
            httpHeaders.add(org.apache.http.HttpHeaders.TIMEOUT, "");
            val exchange = restTemplate.exchange(url, HttpMethod.POST,
                    new HttpEntity<>(JsonUtil.writeValueAsBytes(req), httpHeaders), String.class);
            val responseStatus = exchange.getStatusCodeValue();
            if (responseStatus != HttpStatus.SC_OK) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, SNAPSHOT_TABLE_CHECK_ERROR_MESSAGE, project, tableIdentity));
            }
            val responseBody = Optional.ofNullable(exchange.getBody()).orElse("");
            val response = JsonUtil.readValue(responseBody,
                    new TypeReference<RestResponse<SnapshotSourceTableStatsResponse>>() {
                    });
            if (!StringUtils.equals(response.getCode(), KylinException.CODE_SUCCESS)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, SNAPSHOT_TABLE_CHECK_ERROR_MESSAGE, project, tableIdentity));
            }
            val needRefreshPartitionsValue = response.getData().getNeedRefreshPartitionsValue();
            val needRefresh = response.getData().getNeedRefresh();
            log.info("source table[{}] needRefresh[{}], needRefreshPartitionsValue[{}]", tableIdentity, needRefresh,
                    needRefreshPartitionsValue);
            val result = new CheckSourceTableResult();
            result.setTableIdentity(tableIdentity);
            result.setNeedRefresh(needRefresh);
            result.setNeedRefreshPartitionsValue(needRefreshPartitionsValue);
            checkSourceTableQueue.offer(result);
        } catch (IOException e) {
            throw new KylinRuntimeException(
                    String.format(Locale.ROOT, SNAPSHOT_TABLE_CHECK_ERROR_MESSAGE, project, tableIdentity), e);
        }
    }
}

@Getter
@Setter
class SnapshotSourceTableStatsResponse {
    @JsonProperty("need_refresh")
    private Boolean needRefresh = Boolean.FALSE;
    @JsonProperty("need_refresh_partitions_value")
    private Set<String> needRefreshPartitionsValue = Sets.newHashSet();
}
