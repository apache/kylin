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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.util.CheckUtil.checkCondition;
import static org.apache.kylin.rest.cache.RedisCache.checkRedisClient;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.KylinEhCache;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache;
import org.apache.kylin.rest.cache.RedisCache;
import org.apache.kylin.rest.cache.RedisCacheV2;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.TableMetaCacheResult;
import org.apache.kylin.rest.response.TableMetaCacheResultV2;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import lombok.val;

/**
 * query cache manager that
 * 1. holding query cache - <SQlRequest, SQLResponse> pairs
 * 2. holding schema cache - <UserName, Schema> pairs
 */
@Component("queryCacheManager")
public class QueryCacheManager implements CommonQueryCacheSupporter {

    //    public enum Type {
    //        SUCCESS_QUERY_CACHE("StorageCache"), EXCEPTION_QUERY_CACHE("ExceptionQueryCache"), SCHEMA_CACHE("SchemaCache");
    //
    //        public String rootCacheName;
    //
    //        Type(String rootCacheName) {
    //            this.rootCacheName = rootCacheName;
    //        }
    //    }

    private static final Logger logger = LoggerFactory.getLogger("query");

    private KylinCache kylinCache;

    @PostConstruct
    public void init() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isRedisEnabled()) {
            if (kylinConfig.isRedisSentinelEnabled()) {
                kylinCache = RedisCacheV2.getInstance();
            } else {
                kylinCache = RedisCache.getInstance();
            }
        } else if (kylinConfig.isMemcachedEnabled()) {
            kylinCache = CompositeMemcachedCache.getInstance();
        }
        if (kylinCache == null) {
            kylinCache = KylinEhCache.getInstance();
        }
        if (kylinCache instanceof RedisCache && checkRedisClient()) {
            logger.info("Redis cache connect successfully!");
        }
    }

    /**
     * check if the sqlResponse is qualified for caching
     * @param sqlResponse
     * @return
     */
    private boolean cacheable(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
        long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
        long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
        long responseSize = sqlResponse.getResultRowCount() > 0
                ? sqlResponse.getResultRowCount() * sqlResponse.getColumnMetas().size()
                : 0;
        return checkCondition(QueryUtil.isSelectStatement(sqlRequest.getSql()), "query is non-select")
                && checkCondition(!sqlResponse.isException(), "query has exception") //
                && checkCondition(!sqlResponse.isQueryPushDown() || kylinConfig.isPushdownQueryCacheEnabled(),
                        "query is executed with pushdown, or the cache for pushdown is disabled") //
                && checkCondition(
                        sqlResponse.getDuration() > durationThreshold
                                || sqlResponse.getTotalScanRows() > scanCountThreshold
                                || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                        "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                        sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanRows(),
                        scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                && checkCondition(responseSize < kylinConfig.getLargeQueryThreshold(),
                        "query response is too large: {} ({})", responseSize, kylinConfig.getLargeQueryThreshold());
    }

    public void doCacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        try {
            sqlResponse.readAllRows();
            kylinCache.put(Type.SUCCESS_QUERY_CACHE.rootCacheName, sqlRequest.getProject(), sqlRequest.getCacheKey(),
                    sqlResponse);
        } catch (Exception e) {
            logger.error("[query cache log] Error caching result of success query {}", sqlRequest.getSql(), e);
        }
    }

    public void cacheSuccessQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        if (cacheable(sqlRequest, sqlResponse)) {
            doCacheSuccessQuery(sqlRequest, sqlResponse);
        }
    }

    public void cacheFailedQuery(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        try {
            kylinCache.put(Type.EXCEPTION_QUERY_CACHE.rootCacheName, sqlRequest.getProject(), sqlRequest.getCacheKey(),
                    sqlResponse);
        } catch (Exception e) {
            logger.error("[query cache log] Error caching result of failed query {}", sqlRequest.getSql(), e);
        }
    }

    public void putIntoExceptionCache(SQLRequest req, SQLResponse resp) {
        try {
            kylinCache.put(Type.EXCEPTION_QUERY_CACHE.rootCacheName, req.getProject(), req.getCacheKey(), resp);
        } catch (Exception e) {
            logger.error("ignore cache error", e);
        }
    }

    public void updateIntoExceptionCache(SQLRequest req, SQLResponse resp) {
        try {
            kylinCache.update(Type.EXCEPTION_QUERY_CACHE.rootCacheName, req.getProject(), req.getCacheKey(), resp);
        } catch (Exception e) {
            logger.error("ignore cache error", e);
        }
    }

    public SQLResponse getFromExceptionCache(SQLRequest req) {
        return searchFailedCache(req);
    }

    public SQLResponse doSearchQuery(QueryCacheManager.Type type, SQLRequest sqlRequest) {
        Object response = getCache(type.rootCacheName, sqlRequest.getProject(), sqlRequest.getCacheKey());
        logger.debug("[query cache log] The cache key is: {}", sqlRequest.getCacheKey());
        if (response == null) {
            return null;
        }
        return (SQLResponse) response;
    }

    public SQLResponse searchSuccessCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.SUCCESS_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            logger.info("[query cache log] No success cache searched");
            return null;
        }

        // check signature for success query resp in case the datasource is changed
        if (QueryCacheSignatureUtil.checkCacheExpired(cached, sqlRequest.getProject())) {
            logger.debug("[query cache log] cache has expired, cache key is {}", sqlRequest.getCacheKey());
            clearQueryCache(sqlRequest);
            return null;
        }

        cached.setStorageCacheUsed(true);
        QueryContext.current().getQueryTagInfo().setStorageCacheUsed(true);
        String cacheType = getCacheType();
        cached.setStorageCacheType(cacheType);
        QueryContext.current().getQueryTagInfo().setStorageCacheType(cacheType);

        val realizations = cached.getNativeRealizations();
        String project = sqlRequest.getProject();
        for (NativeQueryRealization nativeQueryRealization : realizations) {
            if (nativeQueryRealization.getType().equals(QueryMetrics.AGG_INDEX)
                    || nativeQueryRealization.getType().equals(QueryMetrics.TABLE_INDEX)) {
                val modelId = nativeQueryRealization.getModelId();
                val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .getDataflow(modelId);
                nativeQueryRealization.setModelAlias(dataflow.getModelAlias());
            }
        }

        return cached;
    }

    private String getCacheType() {
        if (KylinConfig.getInstanceFromEnv().isRedisEnabled()) {
            return "Redis";
        } else if (KylinConfig.getInstanceFromEnv().isMemcachedEnabled()) {
            return "Memcached";
        } else {
            return "Ehcache";
        }
    }

    public SQLResponse searchFailedCache(SQLRequest sqlRequest) {
        SQLResponse cached = doSearchQuery(Type.EXCEPTION_QUERY_CACHE, sqlRequest);
        if (cached == null) {
            logger.info("[query cache log] No failed cache searched");
            return null;
        }
        cached.setHitExceptionCache(true);
        QueryContext.current().getQueryTagInfo().setHitExceptionCache(true);
        return cached;
    }

    /**
     * search query in both success and failed query cache
     * for success cache, the cached result will be returned only if it passes the expiration check
     * @param sqlRequest
     * @return
     */
    public SQLResponse searchQuery(SQLRequest sqlRequest) {
        SQLResponse cached = searchSuccessCache(sqlRequest);
        if (cached != null) {
            return cached;
        }
        return searchFailedCache(sqlRequest);
    }

    @SuppressWarnings("unchecked")
    public List<TableMeta> getSchemaCache(String project, String userName) {
        TableMetaCacheResult cacheResult = doGetSchemaCache(project, userName);
        if (cacheResult == null) {
            return null;
        }
        if (QueryCacheSignatureUtil.checkCacheExpired(cacheResult.getTables(), cacheResult.getSignature(), project,
                null)) {
            logger.info("[schema cache log] cache has expired, cache key is {}", userName);
            clearSchemaCache(project, userName);
            return null;
        }
        return cacheResult.getTableMetaList();
    }

    public TableMetaCacheResult doGetSchemaCache(String project, String userName) {
        Object metaList = getCache(Type.SCHEMA_CACHE.rootCacheName, project, userName);
        if (metaList == null) {
            return null;
        }
        return (TableMetaCacheResult) metaList;
    }

    public void putSchemaCache(String project, String userName, TableMetaCacheResult schemas) {
        kylinCache.put(Type.SCHEMA_CACHE.rootCacheName, project, userName, schemas);
    }

    @SuppressWarnings("unchecked")
    public List<TableMetaWithType> getSchemaV2Cache(String project, String modelName, String userName) {
        TableMetaCacheResultV2 cacheResult = doGetSchemaCacheV2(project, modelName, userName);
        if (cacheResult == null) {
            return null;
        }
        if (QueryCacheSignatureUtil.checkCacheExpired(cacheResult.getTables(), cacheResult.getSignature(), project,
                modelName)) {
            logger.info("[schema cache log] cache has expired, cache key is {}", userName);
            clearSchemaCacheV2(project, userName);
            return null;
        }

        return cacheResult.getTableMetaList();
    }

    public TableMetaCacheResultV2 doGetSchemaCacheV2(String project, String modelName, String userName) {
        String cacheKey = userName + "v2";
        if (modelName != null) {
            cacheKey = cacheKey + modelName;
        }
        Object metaList = getCache(Type.SCHEMA_CACHE.rootCacheName, project, cacheKey);
        if (metaList == null) {
            return null;
        }
        return (TableMetaCacheResultV2) metaList;
    }

    public void putSchemaV2Cache(String project, String modelName, String userName, TableMetaCacheResultV2 schemas) {
        String cacheKey = userName + "v2";
        if (modelName != null) {
            cacheKey = cacheKey + modelName;
        }
        putCache(Type.SCHEMA_CACHE.rootCacheName, project, cacheKey, schemas);
    }

    public void clearSchemaCacheV2(String project, String userName) {
        removeCache(Type.SCHEMA_CACHE.rootCacheName, project, userName + "v2");
    }

    public void clearSchemaCache(String project, String userName) {
        removeCache(Type.SCHEMA_CACHE.rootCacheName, project, userName);
    }

    public void onClearSchemaCache(String project) {
        clearSchemaCache(project);
    }

    public void clearSchemaCache(String project) {
        clearCacheByType(Type.SCHEMA_CACHE.rootCacheName, project);
    }

    public void clearQueryCache(SQLRequest request) {
        removeCache(Type.SUCCESS_QUERY_CACHE.rootCacheName, request.getProject(), request.getCacheKey());
        removeCache(Type.EXCEPTION_QUERY_CACHE.rootCacheName, request.getProject(), request.getCacheKey());
    }

    public void onClearProjectCache(String project) {
        clearProjectCache(project);
    }

    public void clearProjectCache(String project) {
        if (project == null) {
            logger.info("[query cache log] clear query cache for all projects.");
            clearAllCache();
        } else {
            logger.info("[query cache log] clear query cache for {}", project);
            clearCacheByType(Type.SUCCESS_QUERY_CACHE.rootCacheName, project);
            clearCacheByType(Type.EXCEPTION_QUERY_CACHE.rootCacheName, project);
            clearCacheByType(Type.SCHEMA_CACHE.rootCacheName, project);
        }
    }

    public boolean recoverCache() {
        logger.info("Redis client recovery start.");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // no need to recover
        if (!kylinConfig.isRedisEnabled()) {
            logger.info("No need to recover Redis client, isRedisEnabled:{}", kylinConfig.isRedisEnabled());
            return false;
        }

        if (!kylinConfig.isRedisSentinelEnabled()) {
            // cluster、standalone
            kylinCache = RedisCache.recoverInstance();
        } else {
            // sentinel
            if (kylinCache instanceof RedisCacheV2) {
                kylinCache = ((RedisCacheV2) kylinCache).recoverInstance();
            } else {
                kylinCache = RedisCacheV2.getInstance();
            }
        }

        // recover failed，use Ehcache
        if (kylinCache == null) {
            logger.info("Redis client recover failed, use ehcache instead.");
            kylinCache = KylinEhCache.getInstance();
            return false;
        } else {
            logger.info("Redis client recover successfully.");
            return true;
        }
    }

    //for test
    public KylinCache getCache() {
        return kylinCache;
    }

    public Object getCache(String type, String project, Object key) {
        try {
            return kylinCache.get(type, project, key);
        } catch (Exception e) {
            logger.error("Get cache failed, type:{}, project:{}, key:{}, exception:{}", type, project, key, e);
        }
        return null;
    }

    public void putCache(String type, String project, Object key, Object value) {
        try {
            kylinCache.put(type, project, key, value);
        } catch (Exception e) {
            logger.error("Put cache failed, type:{}, project:{}, key:{}, value:{}, exception:{}", type, project, key,
                    value, e);
        }
    }

    public boolean removeCache(String type, String project, Object key) {
        try {
            return kylinCache.remove(type, project, key);
        } catch (Exception e) {
            logger.error("Remove cache failed, type:{}, project:{}, key:{}, exception:{}", type, project, key, e);
        }
        return false;
    }

    public void clearAllCache() {
        try {
            kylinCache.clearAll();
        } catch (Exception e) {
            logger.error("Clear all cache failed, exception:", e);
        }
    }

    public void clearCacheByType(String type, String project) {
        try {
            kylinCache.clearByType(type, project);
        } catch (Exception e) {
            logger.error("Clear cache by type failed, type:{}, project:{}, exception:{}", type, project, e);
        }
    }
}
