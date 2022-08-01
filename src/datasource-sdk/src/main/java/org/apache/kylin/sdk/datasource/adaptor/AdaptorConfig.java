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
package org.apache.kylin.sdk.datasource.adaptor;

import java.util.Map;

import lombok.Getter;

public class AdaptorConfig {
    public final String url;
    public final String driver;
    public final String username;
    public final String password;

    public String datasourceId;
    public int poolMaxIdle = 10;
    public int poolMaxTotal = 10;
    public int poolMinIdle = 2;
    public long timeBetweenEvictionRunsMillis = 15000;
    public long maxWait = 60000;

    private int connectRetryTimes = 1;
    private long sleepMillisecBetweenRetry = 100;

    @Getter
    public Map<String, String> options;

    public AdaptorConfig(String url, String driver, String username, String password, Map<String, String> options) {
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
        this.options = options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AdaptorConfig that = (AdaptorConfig) o;

        if (poolMaxIdle != that.poolMaxIdle)
            return false;
        if (poolMaxTotal != that.poolMaxTotal)
            return false;
        if (poolMinIdle != that.poolMinIdle)
            return false;
        if (!url.equals(that.url))
            return false;
        if (!driver.equals(that.driver))
            return false;
        if (!username.equals(that.username))
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        return datasourceId != null ? datasourceId.equals(that.datasourceId) : that.datasourceId == null;
    }

    @Override
    public int hashCode() {
        int result = url.hashCode();
        result = 31 * result + driver.hashCode();
        result = 31 * result + username.hashCode();
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (datasourceId != null ? datasourceId.hashCode() : 0);
        result = 31 * result + poolMaxIdle;
        result = 31 * result + poolMaxTotal;
        result = 31 * result + poolMinIdle;
        return result;
    }

    public int getConnectRetryTimes() {
        return connectRetryTimes;
    }

    public void setConnectRetryTimes(int connectRetryTimes) {
        this.connectRetryTimes = connectRetryTimes;
    }

    public long getSleepMillisecBetweenRetry() {
        return sleepMillisecBetweenRetry;
    }

    public void setSleepMillisecBetweenRetry(long sleepMillisecBetweenRetry) {
        this.sleepMillisecBetweenRetry = sleepMillisecBetweenRetry;
    }
}
