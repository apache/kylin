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

import com.google.common.base.Joiner;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.execution.ExecutableState;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class MailNotificationUtil {
    public static final String JOB_ERROR = "JOB_ERROR";
    public static final String JOB_DISCARD = "JOB_DISCARD";
    public static final String JOB_SUCCEED = "JOB_SUCCEED";
    public static final String JOB_SUICIDAL = "JOB_SUICIDAL";
    public static final String JOB_LOAD_EMPTY_DATA = "LOAD_EMPTY_DATA";
    public static final String JOB_SOURCE_RECORDS_CHANGE = "SOURCE_RECORDS_CHANGE";
    public static final String METADATA_PERSIST_FAIL = "METADATA_PERSIST_FAIL";

    public static final String NA = "NA";

    private static String localHostName;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            localHostName = "UNKNOWN";
        }
    }

    private MailNotificationUtil() {
        throw new IllegalStateException("Class MailNotificationUtil is an utility class !");
    }

    private static String getMailTemplateKey(ExecutableState state) {
        switch (state) {
            case ERROR:
                return JOB_ERROR;
            case DISCARDED:
                return JOB_DISCARD;
            case SUCCEED:
                return JOB_SUCCEED;
            case SUICIDAL:
                return JOB_SUICIDAL;
            default:
                return null;
        }
    }

    private static String getMailTemplateKey(JobIssueEnum jobIssue) {
        switch (jobIssue) {
            case LOAD_EMPTY_DATA:
                return JOB_LOAD_EMPTY_DATA;
            case SOURCE_RECORDS_CHANGE:
                return JOB_SOURCE_RECORDS_CHANGE;
            default:
                return null;
        }
    }


    public static String getLocalHostName() {
        return localHostName;
    }

    public static String getMailContent(ExecutableState state, Map<String, Object> dataMap) {
        return MailTemplateProvider.getInstance().buildMailContent(MailNotificationUtil.getMailTemplateKey(state),
                dataMap);
    }

    public static String getMailContent(JobIssueEnum jobIssue, Map<String, Object> dataMap) {
        return MailTemplateProvider.getInstance().buildMailContent(MailNotificationUtil.getMailTemplateKey(jobIssue),
                dataMap);
    }

    public static String getMailContent(String key, Map<String, Object> dataMap) {
        return MailTemplateProvider.getInstance().buildMailContent(key, dataMap);
    }

    public static String getMailTitle(String... titleParts) {
        return "[" + Joiner.on("]-[").join(titleParts) + "]";
    }

}
