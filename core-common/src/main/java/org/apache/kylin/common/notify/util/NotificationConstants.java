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

package org.apache.kylin.common.notify.util;

public class NotificationConstants {
    public static final String NOTIFY_EMAIL_LIST = "notify_email_list";
    public static final String NOTIFY_DINGTALK_LIST = "notify_dingtalk_list";

    /**
     * The following task status needs to be notified
     */
    public static final String JOB_ERROR = "ERROR";
    public static final String JOB_DISCARDED = "DISCARDED";
    public static final String JOB_SUCCEED = "SUCCEED";
    public static final String JOB_MIGRATION_REQUEST = "MIGRATION_REQUEST";
    public static final String JOB_MIGRATION_REJECTED = "MIGRATION_REJECTED";
    public static final String JOB_MIGRATION_APPROVED = "MIGRATION_APPROVED";
    public static final String JOB_MIGRATION_COMPLETED = "MIGRATION_COMPLETED";
    public static final String JOB_MIGRATION_FAILED = "MIGRATION_FAILED";
    public static final String JOB_METADATA_PERSIST_FAIL = "METADATA_PERSIST_FAIL";

    /**
     * Template for sending notification
     */
    public static final String JOB_ERROR_NOTIFICATION_TEMP = "JOB_ERROR";
    public static final String JOB_DISCARD_NOTIFICATION_TEMP = "JOB_DISCARD";
    public static final String JOB_SUCCEED_NOTIFICATION_TEMP  = "JOB_SUCCEED";
    public static final String JOB_MIGRATION_REQUEST_TEMP = "MIGRATION_REQUEST";
    public static final String JOB_MIGRATION_REJECTED_TEMP = "MIGRATION_REJECTED";
    public static final String JOB_MIGRATION_APPROVED_TEMP = "MIGRATION_APPROVED";
    public static final String JOB_MIGRATION_COMPLETED_TEMP = "MIGRATION_COMPLETED";
    public static final String JOB_MIGRATION_FAILED_TEMP = "MIGRATION_FAILED";
    public static final String JOB_METADATA_PERSIST_FAIL_TEMP = "METADATA_PERSIST_FAIL";

    public static final String NA = "NA";
}