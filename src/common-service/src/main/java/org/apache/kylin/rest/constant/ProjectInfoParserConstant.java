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
package org.apache.kylin.rest.constant;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;

import lombok.val;

public class ProjectInfoParserConstant {
    public static final ProjectInfoParserConstant INSTANCE = new ProjectInfoParserConstant();
    public final List<String> PROJECT_PARSER_URI_LIST;
    public final List<String> PROJECT_PARSER_URI_EXCLUDED_LIST;

    private ProjectInfoParserConstant() {
        ImmutableList.Builder<String> urisBuilder = ImmutableList.builder();
        constructUris(urisBuilder);
        PROJECT_PARSER_URI_LIST = urisBuilder.build();

        ImmutableList.Builder<String> uriExcludedBuilder = ImmutableList.builder();
        constructExcludeUris(uriExcludedBuilder);
        PROJECT_PARSER_URI_EXCLUDED_LIST = uriExcludedBuilder.build();
    }

    private void constructExcludeUris(final ImmutableList.Builder<String> uriExcludedBuilder) {
        List<String> projectsSubUris = Arrays.asList("statistics", "acceleration", "acceleration_tag",
                "config/deletion", "default_configs");
        projectsSubUris.forEach(projectsSubUri -> uriExcludedBuilder
                .add(format(Locale.ROOT, "/kylin/api/projects/%s", projectsSubUri)));
    }

    private void constructUris(final ImmutableList.Builder<String> urisBuilder) {

        //  /kylin/api/projects/{project}/XXXX/
        {
            val projectsSubUris = Arrays.asList("backup", "default_database", "query_accelerate_threshold",
                    "storage_volume_info", "storage", "storage_quota", "favorite_rules", "statistics", "acceleration",
                    "shard_num_config", "garbage_cleanup_config", "job_notification_config", "push_down_config",
                    "scd2_config", "push_down_project_config", "snapshot_config", "computed_column_config",
                    "segment_config", "project_general_info", "project_config", "source_type", "yarn_queue",
                    "project_kerberos_info", "owner", "config", "jdbc_config");

            projectsSubUris.forEach(projectsSubUri -> urisBuilder
                    .add(format(Locale.ROOT, "/kylin/api/projects/{project}/%s", projectsSubUri)));

            urisBuilder.add("/kylin/api/projects/{project}");
        }

        {
            urisBuilder.add("/kylin/api/models/{project}/{model}/model_desc");
            urisBuilder.add("/kylin/api/models/{project}/{model}/partition_desc");
        }

        // application/vnd.apache.kylin-v2+json
        {
            urisBuilder.add("/api/access/{type}/{project}");
        }
    }
}
