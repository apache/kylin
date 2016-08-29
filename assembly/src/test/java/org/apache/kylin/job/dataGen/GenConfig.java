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

package org.apache.kylin.job.dataGen;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class GenConfig {

    @JsonProperty("columnConfigs")
    private ArrayList<ColumnConfig> columnConfigs;

    @JsonProperty("differentiateBoundary")
    private String differentiateBoundary; //data before and after the provided date will be different, so that different segments will have different segments

    private HashMap<String, ColumnConfig> cache = new HashMap<String, ColumnConfig>();

    public String getDifferentiateBoundary() {
        return differentiateBoundary;
    }

    public void setDifferentiateBoundary(String differentiateBoundary) {
        this.differentiateBoundary = differentiateBoundary;
    }

    public ArrayList<ColumnConfig> getColumnConfigs() {
        return columnConfigs;
    }

    public void setColumnConfigs(ArrayList<ColumnConfig> columnConfigs) {
        this.columnConfigs = columnConfigs;
    }

    public ColumnConfig getColumnConfigByName(String columnName) {
        columnName = columnName.toLowerCase();

        if (cache.containsKey(columnName))
            return cache.get(columnName);

        for (ColumnConfig cConfig : columnConfigs) {
            if (cConfig.getColumnName().toLowerCase().equals(columnName)) {
                cache.put(columnName, cConfig);
                return cConfig;
            }
        }
        cache.put(columnName, null);
        return null;
    }

    public static GenConfig loadConfig(InputStream stream) {
        try {
            GenConfig config = JsonUtil.readValue(stream, GenConfig.class);
            return config;
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
