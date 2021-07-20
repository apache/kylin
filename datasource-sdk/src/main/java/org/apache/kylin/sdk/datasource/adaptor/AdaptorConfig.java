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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptorConfig {
    private static final Logger logger = LoggerFactory.getLogger(AdaptorConfig.class);
    public static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile=true";
    public static final String AUTO_DESERIALIZE = "autoDeserialize=true";
    public static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile=true";
    public static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile=true";
    private static final String APPEND_PARAMS = "allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false";

    public final String url;
    public final String driver;
    public final String username;
    public final String password;

    public String datasourceId;
    public int poolMaxIdle = 8;
    public int poolMaxTotal = 8;
    public int poolMinIdle = 0;

    public AdaptorConfig(String url, String driver, String username, String password) {
        if (url.startsWith("jdbc:mysql")) {
            this.url = filterUrl(url);
            this.username = filterUser(username);
            this.password = filterPassword(password);
        } else {
            this.url = url;
            this.username = username;
            this.password = password;
        }
        this.driver = driver;
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

    protected String filterPassword(String password) {
        if (password.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in password field is filtered", AUTO_DESERIALIZE);
            password = password.replace(AUTO_DESERIALIZE, "");
        }
        return password;
    }

    protected String filterUser(String user) {
        if (user.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in username field is filtered", AUTO_DESERIALIZE);
            user = user.replace(AUTO_DESERIALIZE, "");
        }
        logger.debug("username : {}", user);
        return user;
    }

    protected String filterUrl(String url) {
        if (url.contains("?")) {
            if (url.contains(ALLOW_LOAD_LOCAL_IN_FILE_NAME)) {
                url = url.replace(ALLOW_LOAD_LOCAL_IN_FILE_NAME, "allowLoadLocalInfile=false");
            }
            if (url.contains(AUTO_DESERIALIZE)) {
                url = url.replace(AUTO_DESERIALIZE, "autoDeserialize=false");
            }
            if (url.contains(ALLOW_LOCAL_IN_FILE_NAME)) {
                url = url.replace(ALLOW_LOCAL_IN_FILE_NAME, "allowLocalInfile=false");
            }
            if (url.contains(ALLOW_URL_IN_LOCAL_IN_FILE_NAME)) {
                url = url.replace(ALLOW_URL_IN_LOCAL_IN_FILE_NAME, "allowUrlInLocalInfile=false");
            }
        } else {
            url = url + "?" + APPEND_PARAMS;
        }
        return url;
    }
}
