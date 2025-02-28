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
package org.apache.kylin.cache.kylin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cache.fs.AbstractCacheFileSystem;
import org.apache.kylin.cache.fs.CacheFileSystemConstants;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinCacheFileSystem extends AbstractCacheFileSystem {

    private static final Pattern ACCEPT_CACHE_TIME_PATTERN = Pattern.compile("ACCEPT_CACHE_TIME\\([^()]*\\)");

    /**
     * Check whether it needs to cache data on the current executor
     */
    @Override
    protected boolean isUseLocalCacheForCurrentExecutor() {
        if (null == TaskContext.get()) {
            log.warn("Task Context is null.");
            return false;
        }
        String localCacheForCurrExecutor = TaskContext.get()
                .getLocalProperty(CacheFileSystemConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES);

        if (StringUtils.isBlank(localCacheForCurrExecutor))
            return true; // let the empty default be TRUE
        else
            return Boolean.parseBoolean(localCacheForCurrExecutor);
    }

    @Override
    protected long getAcceptCacheTime() {
        return getAcceptCacheTimeLocally();
    }

    /**
     * To instruct local cache eviction, sql can contain a hint like
     * <pre>
     *      select * from xxx &#47;*+ ACCEPT_CACHE_TIME(158176387682000) *&#47;
     * </pre>
     * <p/>
     * The value is carried to Spark local property, naming "spark.kylin.local-cache.accept-cache-time",
     * and then is used by AbstractCacheFileSystem
     * <p/>
     * @param hintStr The hint string, i.e. &#47;*+ ACCEPT_CACHE_TIME(158176387682000) *&#47;
     * @return 1st param of ACCEPT_CACHE_TIME
     */
    public static String processAcceptCacheTimeInHintStr(String hintStr) {
        String time = extractAcceptCacheTime(hintStr);
        setAcceptCacheTimeLocally(time);
        return time;
    }

    public static String extractAcceptCacheTime(String hintStr) {
        String time = null;
        if (StringUtils.isNotBlank(hintStr)) {
            Matcher matcher = ACCEPT_CACHE_TIME_PATTERN.matcher(hintStr);
            if (matcher.find(0)) {
                String acceptCacheTimeHint = matcher.group();
                time = extractTime(acceptCacheTimeHint);
            }
        }
        return time;
    }

    public static String extractTime(String hint) {
        if (StringUtils.isBlank(hint) || hint.indexOf("ACCEPT_CACHE_TIME(") != 0) {
            return null;
        }
        String[] parts = hint.replace("ACCEPT_CACHE_TIME(", "").replace(")", "").split(",");
        if (parts.length < 1) {
            return null;
        }
        return parts[0];
    }

    public static void setAcceptCacheTimeLocally(String acceptCacheTime) {
        setAcceptCacheTimeLocally(
                acceptCacheTime == null ? System.currentTimeMillis() : Long.parseLong(acceptCacheTime));
    }

    /**
     * User can specify that any cache before the required time must be cleared.
     */
    public static void setAcceptCacheTimeLocally(long acceptCacheTime) {
        Preconditions.checkState(SparkSession.getDefaultSession().isDefined());
        SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
        String key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME;

        long now = System.currentTimeMillis();
        if (acceptCacheTime > now + 1000) { // +1000 for some error tolerance of time
            log.error("Accept-cache-time {} is later than clock time {}. Please check the time sync across machines.",
                    acceptCacheTime, now);
            acceptCacheTime = now;
        }

        // if set multiple times, take the max
        if (sparkContext.getLocalProperty(key) != null) {
            acceptCacheTime = Math.max(acceptCacheTime, Long.parseLong(sparkContext.getLocalProperty(key)));
        }

        sparkContext.setLocalProperty(key, Long.toString(acceptCacheTime));
        sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME_FOR_GLUTEN,
                Long.toString(acceptCacheTime));
    }

    public static void clearAcceptCacheTimeLocally() {
        Preconditions.checkState(SparkSession.getDefaultSession().isDefined());
        SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
        sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME, null);
        sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME_FOR_GLUTEN, null);

        if (TaskContext.get() != null) { // should not have TaskContext, just in case..
            TaskContext.get().getLocalProperties().remove(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME);
            TaskContext.get().getLocalProperties()
                    .remove(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME_FOR_GLUTEN);
        }
    }

    public static long getAcceptCacheTimeLocally() {
        String key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME;

        // defaults to get latest data
        long ret = System.currentTimeMillis();

        if (TaskContext.get() != null) {
            String prop = TaskContext.get().getLocalProperty(key);
            if (prop != null)
                ret = Long.parseLong(prop);
        } else if (SparkSession.getDefaultSession().isDefined()) {
            SparkContext ctx = SparkSession.getDefaultSession().get().sparkContext();
            String prop = ctx.getLocalProperty(key);
            if (prop != null)
                ret = Long.parseLong(prop);
        }
        return ret;
    }
}
