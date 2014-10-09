/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.tools;

import com.kylinolap.job.constant.JobStepStatusEnum;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xduo
 *
 */
public class HadoopStatusChecker {

    protected static final Logger log = LoggerFactory.getLogger(HadoopStatusChecker.class);

    private final String yarnUrl;
    private final String mrJobID;
    private final StringBuilder output;

    public HadoopStatusChecker(String yarnUrl, String mrJobID, StringBuilder output) {
        super();
        this.yarnUrl = yarnUrl;
        this.mrJobID = mrJobID;
        this.output = output;
    }

    public JobStepStatusEnum checkStatus() {
        if (null == mrJobID) {
            this.output.append("Skip status check with empty job id..\n");
            return JobStepStatusEnum.WAITING;
        }

        String applicationId = mrJobID.replace("job", "application");
        String url = yarnUrl.replace("${job_id}", applicationId);
        JobStepStatusEnum status = null;
        String checkResponse = null;
        try {
            checkResponse = getHttpResponse(url);
            JsonNode root = new ObjectMapper().readTree(checkResponse);
            RMAppState state = RMAppState.valueOf(root.findValue("state").getTextValue());
            FinalApplicationStatus finalStatus =
                    FinalApplicationStatus.valueOf(root.findValue("finalStatus").getTextValue());

            log.debug("State of Hadoop job: " + mrJobID + ":" + state + "-" + finalStatus);
            output.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date())
                    + " - State of Hadoop job: " + mrJobID + ":" + state + " - " + finalStatus + "\n");

            switch (finalStatus) {
            case SUCCEEDED:
                status = JobStepStatusEnum.FINISHED;
                break;
            case FAILED:
                status = JobStepStatusEnum.ERROR;
                break;
            case KILLED:
                status = JobStepStatusEnum.ERROR;
                break;
            case UNDEFINED:
                switch (state) {
                case NEW:
                case NEW_SAVING:
                case SUBMITTED:
                case ACCEPTED:
                    status = JobStepStatusEnum.WAITING;
                    break;
                case RUNNING:
                    status = JobStepStatusEnum.RUNNING;
                    break;
                case FINAL_SAVING:
                case FINISHING:
                case FINISHED:
                case FAILED:
                case KILLING:
                case KILLED:
                }
                break;
            }

        } catch (Exception e) {
            output.append("Failed to get status from response with url + " + url + "\n");
            output.append("Exception: " + e.getLocalizedMessage() + "\n");
            log.error("Failed to get status from response with url + " + url + "!\n" + checkResponse, e);
            status = JobStepStatusEnum.ERROR;
        }

        return status;
    }

    private String getHttpResponse(String url) throws IOException {
        HttpClient client = new HttpClient();

        String response = null;
        while (response == null) { // follow redirects via 'refresh'
            if (url.startsWith("https://")) {
                registerEasyHttps();
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }

            HttpMethod get = new GetMethod(url);
            client.executeMethod(get);

            String redirect = null;
            Header h = get.getResponseHeader("Refresh");
            if (h != null) {
                String s = h.getValue();
                int cut = s.indexOf("url=");
                if (cut >= 0) {
                    redirect = s.substring(cut + 4);
                }
            }

            if (redirect == null) {
                response = get.getResponseBodyAsString();
                output.append("Job " + mrJobID + " get status check result.\n");
                log.debug("Job " + mrJobID + " get status check result.\n");
            } else {
                url = redirect;
                output.append("Job " + mrJobID + " check redirect url " + url + ".\n");
                log.debug("Job " + mrJobID + " check redirect url " + url + ".\n");
            }

            get.releaseConnection();
        }

        return response;
    }

    private static Protocol EASY_HTTPS = null;

    private static void registerEasyHttps() {
        // by pass all https issue
        if (EASY_HTTPS == null) {
            EASY_HTTPS =
                    new Protocol("https", (ProtocolSocketFactory) new DefaultSslProtocolSocketFactory(), 443);
            Protocol.registerProtocol("https", EASY_HTTPS);
        }
    }

    public JobStepStatusEnum calculateStatus(JobStatus jobStatus) {
        JobStepStatusEnum status;
        switch (jobStatus.getState()) {
        case RUNNING:
            status = JobStepStatusEnum.RUNNING;
            break;
        case SUCCEEDED:
            status = JobStepStatusEnum.FINISHED;
            break;
        case FAILED:
            status = JobStepStatusEnum.ERROR;
            break;
        case PREP:
            status = JobStepStatusEnum.WAITING;
            break;
        case KILLED:
            status = JobStepStatusEnum.ERROR;
            break;
        default:
            status = JobStepStatusEnum.ERROR;
        }

        return status;
    }
}
