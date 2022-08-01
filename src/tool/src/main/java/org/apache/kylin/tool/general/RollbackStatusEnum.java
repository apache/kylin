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

package org.apache.kylin.tool.general;

public enum RollbackStatusEnum {
    START, CHECK_PARAM_SUCCESS, BACKUP_CURRENT_METADATA_SUCCESS, CHECK_CLUSTER_STATUS_SUCESS, FORWARD_TO_USER_TARGET_TIME_FROM_SNAPSHOT_SUCESS, OUTPUT_DIFF_SUCCESS, WAIT_USER_CONFIRM_SUCCESS, CHECK_STORAGE_DATA_AVAILABLE_SUCCESS, RESTORE_MIRROR_SUCCESS
}
