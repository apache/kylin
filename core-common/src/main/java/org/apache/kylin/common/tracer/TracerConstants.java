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

package org.apache.kylin.common.tracer;

import java.util.Locale;

import org.apache.kylin.common.util.StringUtil;

public class TracerConstants {

    public enum OperationEum {
        MAIN("main_query"), //
        FETCH_CACHE_STEP("fetch_cache"), //
        SQL_PARSE_STEP("parse_sql"), //
        QUERY_PLAN_STEP("query_plan"), //
        FETCH_SEGMENT_CACHE_STEP("fetch_segment_cache"), //
        ENDPOINT_RANGE_REQUEST("endpoint_range_request"), //
        REGION_SERVER_RPC("region_server_rpc"), //
        STREAMING_RECEIVER_REQUEST("streaming_receiver_request") //
        ;

        private final String name;

        OperationEum(String name) {
            this.name = name;
        }

        public static OperationEum getByName(String name) {
            if (StringUtil.isEmpty(name)) {
                return null;
            }
            for (OperationEum property : OperationEum.values()) {
                if (property.name.equals(name.toUpperCase(Locale.ROOT))) {
                    return property;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public enum TagEnum {
        PROJECT("project"), //
        SQL("sql"), //
        SUBMITTER("submitter"), //
        QUERY_ID("query_id"), //
        CUBE("cube"), //
        SEGMENT("segment"), //
        HTABLE("htable"), //
        EPRANGE("ep_range"), //
        CUBOID("cuboid"), //
        FUZZY_KEY_SIZE("fuzzykey_size"), //
        REGION_SERVER("region_server"), //
        RPC_DURATION("rpc_duration"), //
        RPC_SCAN_COUNT("rpc_scan_count"), //
        RPC_FILTER_COUNT("rpc_filter_count"), //
        RPC_AGG_COUNT("rpc_agg_count"), //
        RPC_SERIALIZED_BYTES("rpc_serialized_bytes"), //
        RPC_SYSTEMLOAD("rpc_sysload"), //
        RPC_FREE_MEM("rpc_free_physical_mem"), //
        RPC_FREE_SWAP("rpc_free_swap_size"), //
        RPC_ETC_MSG("rpc_etc_message"), //
        REPLICA_SET("replica_set")//
        ;

        private final String name;

        TagEnum(String name) {
            this.name = name;
        }

        public static TagEnum getByName(String name) {
            if (StringUtil.isEmpty(name)) {
                return null;
            }
            for (TagEnum property : TagEnum.values()) {
                if (property.name.equals(name.toUpperCase(Locale.ROOT))) {
                    return property;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
