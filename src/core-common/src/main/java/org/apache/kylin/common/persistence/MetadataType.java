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

package org.apache.kylin.common.persistence;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.resources.AclRawResource;
import org.apache.kylin.common.persistence.resources.CcModelRelationRawResource;
import org.apache.kylin.common.persistence.resources.ComputeColumnRawResource;
import org.apache.kylin.common.persistence.resources.DataParserRawResource;
import org.apache.kylin.common.persistence.resources.DataflowRawResource;
import org.apache.kylin.common.persistence.resources.FusionModelRawResource;
import org.apache.kylin.common.persistence.resources.HistorySourceUsageRawResource;
import org.apache.kylin.common.persistence.resources.IndexPlanRawResource;
import org.apache.kylin.common.persistence.resources.InternalTableRawResource;
import org.apache.kylin.common.persistence.resources.JarInfoRawResource;
import org.apache.kylin.common.persistence.resources.JobStatsRawResource;
import org.apache.kylin.common.persistence.resources.KafkaConfigRawResource;
import org.apache.kylin.common.persistence.resources.LayoutDetailsRawResource;
import org.apache.kylin.common.persistence.resources.LayoutRawResource;
import org.apache.kylin.common.persistence.resources.LogicalViewRawResource;
import org.apache.kylin.common.persistence.resources.ModelRawResource;
import org.apache.kylin.common.persistence.resources.ObjectAclRawResource;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.common.persistence.resources.QueryRecordRawResource;
import org.apache.kylin.common.persistence.resources.RecRawResource;
import org.apache.kylin.common.persistence.resources.ResourceGroupRawResource;
import org.apache.kylin.common.persistence.resources.SegmentRawResource;
import org.apache.kylin.common.persistence.resources.SqlBlacklistRawResource;
import org.apache.kylin.common.persistence.resources.StreamingJobRawResource;
import org.apache.kylin.common.persistence.resources.SystemRawResource;
import org.apache.kylin.common.persistence.resources.TableExdRawResource;
import org.apache.kylin.common.persistence.resources.TableInfoRawResource;
import org.apache.kylin.common.persistence.resources.TableModelRelationRawResource;
import org.apache.kylin.common.persistence.resources.UserGlobalAclRawResource;
import org.apache.kylin.common.persistence.resources.UserGroupRawResource;
import org.apache.kylin.common.persistence.resources.UserInfoRawResource;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.Getter;

/**
 * The enum name should be same as the table name
 */
@Getter
public enum MetadataType {
    ALL(RawResource.class), //
    PROJECT(ProjectRawResource.class), //
    ACL(AclRawResource.class), //
    COMPUTE_COLUMN(ComputeColumnRawResource.class), //
    DATAFLOW(DataflowRawResource.class), //
    HISTORY_SOURCE_USAGE(HistorySourceUsageRawResource.class), //
    INDEX_PLAN(IndexPlanRawResource.class), //
    JOB_STATS(JobStatsRawResource.class), //
    LAYOUT(LayoutRawResource.class), //
    LAYOUT_DETAILS(LayoutDetailsRawResource.class), //
    MODEL(ModelRawResource.class), //
    OBJECT_ACL(ObjectAclRawResource.class), //
    RESOURCE_GROUP(ResourceGroupRawResource.class), //
    SEGMENT(SegmentRawResource.class), //
    INTERNAL_TABLE(InternalTableRawResource.class), //
    TABLE_INFO(TableInfoRawResource.class), //
    TABLE_EXD(TableExdRawResource.class), //
    USER_GLOBAL_ACL(UserGlobalAclRawResource.class), //
    USER_GROUP(UserGroupRawResource.class), //
    USER_INFO(UserInfoRawResource.class), //
    SQL_BLACKLIST(SqlBlacklistRawResource.class), //
    JAR_INFO(JarInfoRawResource.class), //
    DATA_PARSER(DataParserRawResource.class), //
    FUSION_MODEL(FusionModelRawResource.class), //
    KAFKA_CONFIG(KafkaConfigRawResource.class), //
    LOGICAL_VIEW(LogicalViewRawResource.class), //
    STREAMING_JOB(StreamingJobRawResource.class), //
    QUERY_RECORD(QueryRecordRawResource.class), //
    SYSTEM(SystemRawResource.class), //
    CC_MODEL_RELATION(CcModelRelationRawResource.class), //
    TABLE_MODEL_RELATION(TableModelRelationRawResource.class), //
    TMP_REC(RecRawResource.class); //

    public static final Set<MetadataType> NON_GLOBAL_METADATA_TYPE = Collections.unmodifiableSet(
            Sets.newHashSet(ACL, COMPUTE_COLUMN, DATAFLOW, INDEX_PLAN, JOB_STATS, LAYOUT, LAYOUT_DETAILS, MODEL,
                    SEGMENT, TABLE_EXD, TABLE_INFO, SQL_BLACKLIST, JAR_INFO, DATA_PARSER, FUSION_MODEL, KAFKA_CONFIG,
                    LOGICAL_VIEW, STREAMING_JOB, QUERY_RECORD, CC_MODEL_RELATION, TABLE_MODEL_RELATION));

    public static final Set<MetadataType> WITH_PROJECT_PREFIX_METADATA = Collections
            .unmodifiableSet(Sets.newHashSet(TABLE_EXD, TABLE_INFO, KAFKA_CONFIG, JAR_INFO, DATA_PARSER, TMP_REC));

    public static final Set<String> ALL_TYPE_STR = Collections
            .unmodifiableSet(Arrays.stream(MetadataType.values()).map(Enum::name).collect(Collectors.toSet()));

    public static final Set<MetadataType> NEED_CACHED_METADATA = Collections.unmodifiableSet(
            Arrays.stream(MetadataType.values()).filter(type -> type != MetadataType.ALL).collect(Collectors.toSet()));

    public static final Set<MetadataType> CASE_INSENSITIVE_METADATA = Collections
            .unmodifiableSet(Sets.newHashSet(PROJECT, USER_INFO, USER_GROUP));

    public static MetadataType create(String value) {
        try {
            return MetadataType.valueOf(value);
        } catch (Exception e) {
            if (value.equals("_REC")) {
                return MetadataType.TMP_REC;
            }
            throw new IllegalArgumentException(
                    "No enum constant [" + MetadataType.class.getCanonicalName() + "." + value + "]");
        }
    }

    private final Class<? extends RawResource> resourceClass;

    <T extends RawResource> MetadataType(Class<T> resourceClass) {
        this.resourceClass = resourceClass;
    }

    /**
     * Split key with type enum
     * e.g. table/kylin_sales
     * @param keyWithType The key with type
     * @return The pair of type and key
     */
    public static Pair<MetadataType, String> splitKeyWithType(String keyWithType) {
        if (!keyWithType.contains("/")) {
            throw new IllegalArgumentException("Not supported format: " + keyWithType);
        }
        if (keyWithType.equals("/")) {
            return new Pair<>(MetadataType.ALL, keyWithType);
        }
        String[] keyAndType = keyWithType.split("/", 2);
        return new Pair<>(MetadataType.create(keyAndType[0]), keyAndType[1]);
    }

    /**
                   ｜ key contains '/'  ｜ key doesn't contain '/'
     --------------+--------------------+-----------------------------
     type is all   ｜return origin key  ｜ throw exception
     --------------+--------------------+-----------------------------
     type not all  ｜throw exception    ｜ return type + "/" + metaKey
     ------------- +--------------------+-----------------------------
     */
    public static String mergeKeyWithType(String metaKey, MetadataType type) {
        if (type == MetadataType.ALL || metaKey.contains("/")) {
            if (type == MetadataType.ALL && metaKey.contains("/")) {
                return metaKey;
            }
            throw new IllegalArgumentException("Can't merge type & metaKey: " + type + ", " + metaKey);
        }
        return type + "/" + metaKey;
    }
}
