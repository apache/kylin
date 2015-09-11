/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class StreamingConfig extends RootPersistentEntity {

    public static Serializer<StreamingConfig> SERIALIZER = new JsonSerializer<StreamingConfig>(StreamingConfig.class);

    @JsonProperty("name")
    private String name;

    @JsonManagedReference
    @JsonProperty("clusters")
    private List<KafkaClusterConfig> kafkaClusterConfigs;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("timeout")
    private int timeout;

    @JsonProperty("maxReadCount")
    private int maxReadCount;

    @JsonProperty("bufferSize")
    private int bufferSize;

    @JsonProperty("iiName")
    private String iiName;

    @JsonProperty("cubeName")
    private String cubeName;

    @JsonProperty("parserName")
    private String parserName;
    
    //"tsColName=timestamp;x=y"
    @JsonProperty("parserProperties")
    private String parserProperties;

    @JsonProperty("margin")
    private long margin;

    public List<KafkaClusterConfig> getKafkaClusterConfigs() {
        return kafkaClusterConfigs;
    }

    public String getParserName() {
        return parserName;
    }

    public void setParserName(String parserName) {
        this.parserName = parserName;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getMaxReadCount() {
        return maxReadCount;
    }

    public void setMaxReadCount(int maxReadCount) {
        this.maxReadCount = maxReadCount;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getIiName() {
        return iiName;
    }

    public void setIiName(String iiName) {
        this.iiName = iiName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getMargin() {
        return margin;
    }

    public void setMargin(long margin) {
        this.margin = margin;
    }

    public String getParserProperties() {
        return parserProperties;
    }

    public void setParserProperties(String parserProperties) {
        this.parserProperties = parserProperties;
    }

    @Override
    public StreamingConfig clone() {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SERIALIZER.serialize(this, new DataOutputStream(baos));
            return SERIALIZER.deserialize(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        } catch (IOException e) {
            throw new RuntimeException(e);//in mem, should not happen
        }
    }

}
