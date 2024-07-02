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
package org.apache.kylin.job.factory;

/**
 **/
public class JobFactoryConstant {

    private JobFactoryConstant() {
    }

    public static final String ROUTINE_JOB_FACTORY = "ROUTINE_JOB_FACTORY";
    public static final String META_JOB_FACTORY = "META_JOB_FACTORY";
    public static final String SOURCE_USAGE_JOB_FACTORY = "SOURCE_USAGE_JOB_FACTORY";
    public static final String AUTO_REFRESH_JOB_FACTORY = "AUTO_REFRESH_JOB_FACTORY";

    public static final String CUBE_JOB_FACTORY = "CUBE_JOB_FACTORY";
    public static final String MERGE_JOB_FACTORY = "MERGE_JOB_FACTORY";
    public static final String TABLE_SAMPLING_JOB_FACTORY = "TABLE_SAMPLING_JOB_FACTORY";
    public static final String SNAPSHOT_JOB_FACTORY = "SNAPSHOT_JOB_FACTORY";
    public static final String LAYOUT_DATA_OPTIMIZE_JOB_FACTORY = "OPTIMIZE_JOB_FACTORY";
    public static final String INTERNAL_TABLE_JOB_FACTORY = "INTERNAL_TABLE_JOB_FACTORY";
    public static final String INDEX_PLAN_OPTIMIZE_JOB_FACTORY = "INDEX_PLAN_OPTIMIZE_JOB_FACTORY";

}
