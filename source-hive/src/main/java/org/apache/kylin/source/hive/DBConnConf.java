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

package org.apache.kylin.source.hive;

import java.util.Locale;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnConf {
    private static final Logger logger = LoggerFactory.getLogger(DBConnConf.class);
    public static final String KEY_DRIVER = "driver";
    public static final String KEY_URL = "url";
    public static final String KEY_USER = "user";
    public static final String KEY_PASS = "pass";

    public static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile=true";

    public static final String AUTO_DESERIALIZE = "autoDeserialize=true";

    public static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile=true";

    public static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile=true";

    private static final String APPEND_PARAMS = "allowLoadLocalInfile=false&autoDeserialize=false&allowLocalInfile=false&allowUrlInLocalInfile=false";

    private String driver;
    private String url;
    private String user;
    private String pass;

    public DBConnConf() {
    }

    public DBConnConf(String prefix, PropertiesConfiguration pc) {
        driver = pc.getString(prefix + KEY_DRIVER);
        url = pc.getString(prefix + KEY_URL);
        user = pc.getString(prefix + KEY_USER);
        pass = pc.getString(prefix + KEY_PASS);
    }

    public DBConnConf(String driver, String url, String user, String pass) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
    }

    public String toString() {
        return String.format(Locale.ROOT, "%s,%s,%s,%s", driver, url, user, pass);
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        if (url != null && url.startsWith("jdbc:mysql")) {
            url = filterUrl(url);
        }
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        if (user.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in username field is filtered", AUTO_DESERIALIZE);
            user = user.replace(AUTO_DESERIALIZE, "");
        }
        logger.debug("username : {}", user);
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        if (pass.contains(AUTO_DESERIALIZE)) {
            logger.warn("sensitive param : {} in password field is filtered", AUTO_DESERIALIZE);
            pass = pass.replace(AUTO_DESERIALIZE, "");
        }
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    private static String filterUrl(String url) {
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
        logger.debug("jdbc url: " + url);
        return url;
    }
}
