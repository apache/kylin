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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetrics;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SQLResponseV2 implements Serializable {
    protected static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(SQLResponseV2.class);

    // the data type for each column
    private List<SelectedColumnMeta> columnMetas;

    // the results rows, each row contains several columns
    private transient Iterable<List<String>> results;

    // if not select query, only return affected row count
    protected int affectedRowCount;

    // queryTagInfo indicating whether an exception occurred
    @JsonProperty("isException")
    protected boolean isException;

    // if isException, the detailed exception message
    protected String exceptionMessage;

    // if isException, the related Exception
    protected Throwable throwable;

    protected long duration;

    protected boolean isPartial = false;

    private List<Long> scanRows;

    private List<Long> scanBytes;

    private String appMasterURL = "";

    @Getter
    @Setter
    protected int failTimes = -1;

    protected long resultRowCount;

    protected int shufflePartitions;

    protected boolean hitExceptionCache = false;

    protected boolean storageCacheUsed = false;

    protected String storageCacheType;

    @JsonProperty("pushDown")
    protected boolean queryPushDown = false;

    @JsonProperty("is_prepare")
    private boolean isPrepare = false;

    @JsonProperty("is_timeout")
    private boolean isTimeout;

    protected byte[] queryStatistics;

    protected String queryId;

    private String server;

    @JsonProperty("is_stop_by_user")
    private boolean isStopByUser;

    @Setter
    @Getter
    private String signature;

    @JsonProperty("realizations")
    private List<NativeQueryRealization> nativeRealizations;

    private String engineType;

    private transient List<SQLResponseTrace> traces;

    // The following additional fields are added to adapter v2 response
    private String cube;
    private long totalScanCount;
    private boolean isSparderUsed;

    public SQLResponseV2() {
    }

    public SQLResponseV2(SQLResponse sqlResponse) {
        Preconditions.checkNotNull(sqlResponse);
        this.columnMetas = sqlResponse.getColumnMetas();
        this.results = sqlResponse.getResults();
        this.affectedRowCount = sqlResponse.getAffectedRowCount();
        this.isException = sqlResponse.isException();
        this.exceptionMessage = sqlResponse.getExceptionMessage();
        this.throwable = sqlResponse.getThrowable();
        this.duration = sqlResponse.getDuration();
        this.isPartial = sqlResponse.isPartial();
        this.scanRows = sqlResponse.getScanRows();
        this.scanBytes = sqlResponse.getScanBytes();
        this.appMasterURL = sqlResponse.getAppMasterURL();
        this.failTimes = sqlResponse.getFailTimes();
        this.resultRowCount = sqlResponse.getResultRowCount();
        this.shufflePartitions = sqlResponse.getShufflePartitions();
        this.hitExceptionCache = sqlResponse.isHitExceptionCache();
        this.storageCacheUsed = sqlResponse.isStorageCacheUsed();
        this.storageCacheType = sqlResponse.getStorageCacheType();
        this.queryPushDown = sqlResponse.isQueryPushDown();
        this.isPrepare = sqlResponse.isPrepare();
        this.isTimeout = sqlResponse.isTimeout();
        this.queryStatistics = sqlResponse.getQueryStatistics();
        this.queryId = sqlResponse.getQueryId();
        this.server = sqlResponse.getServer();
        this.isStopByUser = sqlResponse.isStopByUser();
        this.signature = sqlResponse.getSignature();
        this.nativeRealizations = sqlResponse.getNativeRealizations();
        this.engineType = sqlResponse.getEngineType();
        this.traces = sqlResponse.getTraces();
        this.isSparderUsed = CollectionUtils.isNotEmpty(sqlResponse.getNativeRealizations());
        this.cube = adapterCubeField(sqlResponse.getNativeRealizations());
    }

    @JsonIgnore
    public Throwable getThrowable() {
        return throwable;
    }

    public long getTotalScanRows() {
        return QueryContext.calValueWithDefault(scanRows);
    }

    public long getTotalScanBytes() {
        return QueryContext.calValueWithDefault(scanBytes);
    }

    public boolean isSparderUsed() {
        return isSparderUsed;
    }

    public long getTotalScanCount() {
        long totalCount = QueryContext.calValueWithDefault(scanRows);
        return totalCount < 0 ? 0 : totalCount;
    }

    public String adapterCubeField(List<NativeQueryRealization> realizations) {
        if (CollectionUtils.isEmpty(realizations)) {
            return "";
        }
        List<String> relatedModelAlias = realizations.stream()
                .filter(e -> !QueryMetrics.TABLE_INDEX.equals(e.getIndexType()))
                .map(e -> "CUBE[name=" + e.getModelAlias() + "]").distinct().collect(Collectors.toList());
        List<String> relateIndexModelAlias = realizations.stream()
                .filter(e -> QueryMetrics.TABLE_INDEX.equals(e.getIndexType()))
                .map(e -> "INVERTED_INDEX[name=" + e.getModelAlias() + "]").distinct().collect(Collectors.toList());
        StringBuilder stringBuilder = new StringBuilder();
        String join1 = String.join(",", relatedModelAlias);
        if (StringUtils.isNotBlank(join1)) {
            stringBuilder.append(join1);
        }
        String join2 = String.join(",", relateIndexModelAlias);
        if (StringUtils.isNotBlank(join2)) {
            stringBuilder.append(StringUtils.isBlank(stringBuilder.toString()) ? "" : ",").append(join2);
        }
        return stringBuilder.toString();
    }
}
