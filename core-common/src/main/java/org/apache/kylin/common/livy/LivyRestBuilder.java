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

package org.apache.kylin.common.livy;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.SourceConfigurationUtil;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

@Clarification(priority = Clarification.Priority.MINOR, msg = "I guess it should be removed, we may not continue support this  feature.")
public class LivyRestBuilder {
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(LivyRestBuilder.class);

    final private KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
    final private Map<String, String> hiveConfProps = SourceConfigurationUtil.loadHiveConfiguration();

    private String url;
    private LivyTypeEnum livyTypeEnum;

    private Map<String, String> keys;
    private Map<String, String> arrs;
    private Map<String, String> maps;

    private ArrayList<String> args = new ArrayList<>();

    public LivyRestBuilder() {
        url = kylinConfig.getLivyUrl();

        keys = kylinConfig.getLivyKey();
        arrs = kylinConfig.getLivyArr();
        maps = kylinConfig.getLivyMap();
    }

    public String build() throws JSONException {
        try {

            JSONObject postJson = new JSONObject();

            if (LivyTypeEnum.sql.equals(livyTypeEnum)) {
                postJson.put("className", "org.apache.kylin.engine.spark.SparkSqlOnLivyBatch");
                postJson.put("args", args);
            } else if (LivyTypeEnum.job.equals(livyTypeEnum)) {
                postJson.put("className", "org.apache.kylin.common.util.SparkEntry");
                postJson.put("args", args);
            } else {
                throw new IllegalArgumentException("unSupport livy type.");
            }

            //deal conf of key
            keys.forEach((key, value) -> {
                try {
                    postJson.put(key, value);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

            //deal conf of arr
            arrs.forEach((key, value) -> {
                try {
                    postJson.put(key, Lists.newArrayList(value.split(",")));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

            //deal conf of map
            JSONObject confJson = new JSONObject();
            maps.forEach((key, value) -> {
                try {
                    confJson.put(key, value);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
            postJson.put("conf", confJson);

            return postJson.toString();
        } catch (JSONException e) {
            e.printStackTrace();
            throw new JSONException("create livy json error :" + e.getMessage());
        }
    }

    public void overwriteHiveProps(Map<String, String> overwrites) {
        this.hiveConfProps.putAll(overwrites);
    }

    public String parseProps() {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<String, String> prop : hiveConfProps.entrySet()) {
            s.append("set ");
            s.append(prop.getKey());
            s.append("=");
            s.append(prop.getValue());
            s.append("; \n");
        }
        return s.toString();
    }

    public void addArgs(String arg) {
        this.args.add(arg);
    }

    public void addConf(String key, String value) {
        this.maps.put(key, value);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public ArrayList<String> getArgs() {
        return args;
    }

    public void setArgs(ArrayList<String> args) {
        this.args = args;
    }

    public LivyTypeEnum getLivyTypeEnum() {
        return this.livyTypeEnum;
    }

    public void setLivyTypeEnum(LivyTypeEnum livyTypeEnum) {
        this.livyTypeEnum = livyTypeEnum;
    }
}
