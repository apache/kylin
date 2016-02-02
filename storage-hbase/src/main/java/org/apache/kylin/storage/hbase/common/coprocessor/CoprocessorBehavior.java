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

package org.apache.kylin.storage.hbase.common.coprocessor;

/**
 */
public enum CoprocessorBehavior {
    SCAN, //only scan data, used for profiling tuple scan speed. Will not return any result
    SCAN_FILTER, //only scan+filter used,used for profiling filter speed.  Will not return any result
    SCAN_FILTER_AGGR, //aggregate the result.  Will return results
    SCAN_FILTER_AGGR_CHECKMEM, //default full operations. Will return results
}
