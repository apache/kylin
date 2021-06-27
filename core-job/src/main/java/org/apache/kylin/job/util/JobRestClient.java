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

package org.apache.kylin.job.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.job.JobInstance;

import java.io.IOException;
import java.util.HashMap;

public class JobRestClient extends RestClient {
    private static final String JOBS = "/jobs/";

    public JobRestClient(String host, int port, String userName, String password) {
        this(host, port, userName, password, null, null);
    }

    public JobRestClient(String host, int port, String userName, String password, Integer httpConnectionTimeoutMs, Integer httpSocketTimeoutMs) {
        super(host, port, userName, password, httpConnectionTimeoutMs, httpSocketTimeoutMs);
    }

    public JobInstance buildCubeV2(String cubeName, long startTime, long endTime, CubeBuildTypeEnum buildType) throws IOException {
        String url = baseUrl + CUBES + cubeName + "/build";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, String> paraMap = new HashMap<String, String>();
            paraMap.put("startTime", startTime + "");
            paraMap.put("endTime", endTime + "");
            paraMap.put("buildType", buildType.toString());
            String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
            put.setEntity(new StringEntity(jsonMsg, UTF_8));
            response = client.execute(put);
            String result = getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + result + " with build cube url " + url + "\n" + jsonMsg);
            } else {
                return json2JobInstance(result);
            }
        } finally {
            cleanup(put, response);
        }
    }

    protected JobInstance json2JobInstance(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        JobInstance jobInstance = mapper.readValue(json, JobInstance.class);
        return jobInstance;
    }

    public JobInstance getJobStatus(String jobId) throws IOException {
        String url = baseUrl + JOBS + jobId;
        HttpGet get = newGet(url);
        HttpResponse response = null;
        try {
            response = client.execute(get);
            String result = getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + result + " with get job status " + jobId);
            } else {
                return json2JobInstance(result);
            }
        } finally {
            cleanup(get, response);
        }
    }

    public String JobInstance2JsonString(JobInstance jobInstance) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(jobInstance);
        return jsonString;
    }

    public JobInstance resumeJob(String jobId) throws IOException {
        String url = baseUrl + JOBS + jobId + "/resume";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            response = client.execute(put);
            String result = getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + result + " with resume job " + jobId);
            } else {
                return json2JobInstance(result);
            }
        } finally {
            cleanup(put, response);
        }
    }

    public void discardJob(String jobId) throws IOException {
        String url = baseUrl + JOBS + jobId + "/cancel";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            response = client.execute(put);
            String result = getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + result + " with discard job " + jobId);
            }
        } finally {
            cleanup(put, response);
        }
    }
}
