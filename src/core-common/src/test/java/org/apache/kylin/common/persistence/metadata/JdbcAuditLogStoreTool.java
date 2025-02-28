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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.JdbcInfo;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.val;

public class JdbcAuditLogStoreTool {
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String LOCAL_INSTANCE = "127.0.0.1";

    public static AuditLog createProjectAuditLog(String projectName, long mvcc, String uuid, String unitId
            , boolean diffFlag) {
        return createProjectAuditLog(projectName, mvcc, uuid, unitId, null, diffFlag);
    }

    public static AuditLog createProjectAuditLog(String projectName, long mvcc, String uuid, String unitId,
                                                 String modelUuid, boolean diffFlag) {
        AuditLog resource = new AuditLog();
        resource.setResPath("PROJECT/" + projectName);
        resource.setDiffFlag(diffFlag);
        resource.setProject(uuid);
        resource.setInstance(LOCAL_INSTANCE);
        resource.setUnitId(unitId);
        resource.setModelUuid(modelUuid);
        resource.setByteSource(ByteSource.wrap(("{ \"uuid\" : \"" + uuid + "\",\"meta_key\" : \"" + projectName
                + "\",\"name\" : \"" + projectName + "\"}").getBytes(DEFAULT_CHARSET)));
        resource.setTimestamp(System.currentTimeMillis());
        resource.setMvcc(mvcc);
        return resource;
    }

    public static AuditLog createProjectAuditLog(String projectName, long mvcc) {
        return createProjectAuditLog(projectName, mvcc, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                null, false);
    }

    public static AuditLog createProjectAuditLog(String projectName, String modelUuid, long mvcc) {
        return createProjectAuditLog(projectName, mvcc, UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                modelUuid, false);
    }

    public static AuditLog createProjectAuditLog(String projectName, long mvcc, String uuid, String unitId,
            AuditLog before) {
        AuditLog resource = new AuditLog();
        resource.setResPath("PROJECT/" + projectName);
        resource.setDiffFlag(true);
        resource.setUnitId(unitId);
        resource.setProject(uuid);
        resource.setByteSource(ByteSource.wrap(("{ \"uuid\" : \"" + uuid + "\",\"meta_key\" : \"" + projectName
                + "\",\"name\" : \"" + projectName + "\"}").getBytes(DEFAULT_CHARSET)));
        resource.setTimestamp(System.currentTimeMillis());
        resource.setMvcc(mvcc);
        resource.setInstance(LOCAL_INSTANCE);
        ResourceCreateOrUpdateEvent event = (ResourceCreateOrUpdateEvent) Event.fromLog(before);
        ResourceCreateOrUpdateEvent resEvent = (ResourceCreateOrUpdateEvent) Event.fromLog(resource);
        RawResource createdOrUpdated = resEvent.getCreatedOrUpdated();
        createdOrUpdated.fillContentDiffFromRaw(event.getCreatedOrUpdated());
        resource.setByteSource(ByteSource.wrap(createdOrUpdated.getContentDiff()));
        return resource;
    }

    /**
     * It will create 5 events, 3 create, 1 update, 1 delete
     * @return
     */
    public static List<Event> createEvents() {
        val event1 = (ResourceCreateOrUpdateEvent) Event.fromLog(createProjectAuditLog("abc", 0));
        val event2 = (ResourceCreateOrUpdateEvent) Event.fromLog(createProjectAuditLog("abc2", 0));
        val event3 = (ResourceCreateOrUpdateEvent) Event.fromLog(createProjectAuditLog("abc", 1));
        val event4 = (ResourceCreateOrUpdateEvent) Event.fromLog(createProjectAuditLog("abc3", 0));
        val event5 = new ResourceDeleteEvent("PROJECT/abc3");
        event5.setKey("abc3");
        return Lists.newArrayList(event1, event2, event3, event4, event5);
    }

    public static void prepareJdbcAuditLogStore(String projectPrefixName, JdbcTemplate jdbcTemplate, long logNum) {
        val url = getTestConfig().getMetadataUrl();
        val table = url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX;
        for (int i = 0; i < logNum; i++) {
            val projectName = (projectPrefixName != null ? projectPrefixName : "p") + i;
            String unitId = RandomUtil.randomUUIDStr();
            jdbcTemplate.update(String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, table),
                    "PROJECT/" + projectName,
                    ("{ \"uuid\" : \"" + RandomUtil.randomUUIDStr() + "\",\"meta_key\" : \"" + projectName
                            + "\",\"name\" : \"" + projectName + "\"}").getBytes(DEFAULT_CHARSET),
                    System.currentTimeMillis(), 0, unitId, null, null, AddressUtil.getLocalInstance(), projectName, false);
        }
    }

    public static void mockAuditLogForProjectEntry(String uuid, String project, JdbcInfo info, boolean isDel, long mvcc,
                                                   String unitId) {
        val jdbcTemplate = info.getJdbcTemplate();
        val url = getTestConfig().getMetadataUrl();

        Object[] log = isDel
                ? new Object[] { "PROJECT/" + project, null, System.currentTimeMillis(), mvcc, unitId, null, null,
                        LOCAL_INSTANCE, null, false }
                : new Object[] { "PROJECT/" + project,
                        ("{\"name\" : \"" + project + "\",\"uuid\" : \"" + uuid + "\"}").getBytes(DEFAULT_CHARSET),
                        System.currentTimeMillis(), mvcc, unitId, null, null, LOCAL_INSTANCE, null, false };
        List<Object[]> logs = new ArrayList<>();
        logs.add(log);
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX), logs);
    }

    public static void mockAuditLogForProjectEntry(String project, JdbcInfo info, boolean isDel) {
        String unitId = RandomUtil.randomUUIDStr();
        mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), project, info, isDel, 0, unitId);
    }
}
