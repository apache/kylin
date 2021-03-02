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

public interface Notify {
    String NOTIFY_EMAIL_LIST = "notify_email_list";
    String NOTIFY_DINGTALK_LIST = "notify_dingtalk_list";

    String ERROR = "ERROR";
    String DISCARDED = "DISCARDED";
    String SUCCEED = "SUCCEED";
    String JOB_ERROR = "JOB_ERROR";
    String JOB_DISCARD = "JOB_DISCARD";
    String JOB_SUCCEED = "JOB_SUCCEED";
    String MIGRATION_REQUEST = "MIGRATION_REQUEST";
    String MIGRATION_REJECTED = "MIGRATION_REJECTED";
    String MIGRATION_APPROVED = "MIGRATION_APPROVED";
    String MIGRATION_COMPLETED = "MIGRATION_COMPLETED";
    String MIGRATION_FAILED = "MIGRATION_FAILED";
    String METADATA_PERSIST_FAIL = "METADATA_PERSIST_FAIL";

    String NA = "NA";
}