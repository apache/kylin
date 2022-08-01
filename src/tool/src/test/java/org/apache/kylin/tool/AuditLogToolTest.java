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

package org.apache.kylin.tool;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.OptionBuilder;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

public class AuditLogToolTest extends NLocalFileMetadataTestCase {

    private final static String project = "calories";
    private final static String jobId = "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5";
    private final static String AUDIT_LOG_SUFFIX = ".jsonl";
    private final static String TEST_RESTORE_TABLE = "test_audit_log_restore";
    private final static String DATA_DIR = "src/test/resources/ut_audit_log/";

    private static final Option OPTION_START_TIME = OptionBuilder.getInstance().hasArg().withArgName("START_TIMESTAMP")
            .withDescription("Specify the start timestamp (sec) (optional)").isRequired(false).create("startTime");

    private static final Option OPTION_END_TIME = OptionBuilder.getInstance().hasArg().withArgName("END_TIMESTAMP")
            .withDescription("Specify the end timestamp (sec) (optional)").isRequired(false).create("endTime");

    private static final Option OPTION_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job (optional)").isRequired(false).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project (optional)").isRequired(false).create("project");

    private static final Option OPTION_TABLE = OptionBuilder.getInstance().hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table (optional)").isRequired(false).create("table");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepareData();
    }

    @After
    public void teardown() {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    @Test
    public void testDumpJobAuditLog() throws Exception {
        val job = NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
        val junitFolder = temporaryFolder.getRoot();
        val tool = new AuditLogTool(getTestConfig());
        tool.execute(new String[] { "-project", project, "-job", jobId, "-dir", junitFolder.getAbsolutePath() });
        checkJsonl(job.getStartTime(), job.getEndTime(), junitFolder);
    }

    @Test
    public void testDumpFullAuditLog() throws Exception {
        val job = NExecutableManager.getInstance(getTestConfig(), project).getJob(jobId);
        val start = job.getStartTime() + TimeUnit.HOURS.toMillis(-10);
        val end = job.getEndTime() + TimeUnit.HOURS.toMillis(10);
        val junitFolder = temporaryFolder.getRoot();
        val tool = new AuditLogTool(getTestConfig());
        tool.execute(new String[] { "-startTime", String.valueOf(start), "-endTime", String.valueOf(end), "-dir",
                junitFolder.getAbsolutePath() });
        checkJsonl(start, end, junitFolder);
    }

    @Test
    public void testRestoreAuditLog() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        val tool = new AuditLogTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-table", TEST_RESTORE_TABLE, "-dir", DATA_DIR });

        List<File> jsonls = Arrays.asList(Paths.get(DATA_DIR).toFile().listFiles()).stream()
                .filter(f -> f.getName().endsWith(AUDIT_LOG_SUFFIX)).collect(Collectors.toList());
        long before = 0L;
        for (File f : jsonls) {
            before += fileLines(f);
        }
        Assertions.assertThat(before).isGreaterThan(0);
        long after = jdbcTemplate
                .queryForObject(String.format(Locale.ROOT, "select count(1) from %s", TEST_RESTORE_TABLE), Long.class);
        Assertions.assertThat(after).isEqualTo(before);
    }

    @Test
    public void testExtractJob_throwsException() {
        AuditLogTool auditLogTool = new AuditLogTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);

        // test throwing PARAMETER_NOT_SPECIFY "-project"
        when(optionsHelper.hasOption(OPTION_PROJECT)).thenReturn(false);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "extractJob", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040202: \"-project\" is not specified.", e.toString());
        }

        // test throwing PARAMETER_EMPTY "project"
        when(optionsHelper.hasOption(OPTION_PROJECT)).thenReturn(true);
        when(optionsHelper.getOptionValue(OPTION_PROJECT)).thenReturn(null);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "extractJob", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"project\" is empty.", e.toString());
        }

        // test throwing PARAMETER_EMPTY "job"
        when(optionsHelper.hasOption(OPTION_PROJECT)).thenReturn(true);
        when(optionsHelper.getOptionValue(OPTION_PROJECT)).thenReturn("PROJECT_NAME_TEST");
        when(optionsHelper.getOptionValue(OPTION_JOB)).thenReturn(null);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "extractJob", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"job\" is empty.", e.toString());
        }
    }

    @Test
    public void testExtractFull_throwsException() {
        AuditLogTool auditLogTool = new AuditLogTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);

        // test throwing PARAMETER_TIMESTAMP_NOT_SPECIFY "-startTime"
        when(optionsHelper.hasOption(OPTION_START_TIME)).thenReturn(false);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "extractFull", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040203: Parameter \"-startTime\"  is not specified (milliseconds).", e.toString());
        }

        // test throwing PARAMETER_TIMESTAMP_NOT_SPECIFY "-endTime"
        when(optionsHelper.hasOption(OPTION_START_TIME)).thenReturn(true);
        when(optionsHelper.hasOption(OPTION_END_TIME)).thenReturn(false);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "extractFull", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040203: Parameter \"-endTime\"  is not specified (milliseconds).", e.toString());
        }
    }

    @Test
    public void testRestore_throwsException() {
        AuditLogTool auditLogTool = new AuditLogTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);

        // test throwing PARAMETER_NOT_SPECIFY "-table"
        when(optionsHelper.hasOption(OPTION_TABLE)).thenReturn(false);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "restore", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040202: \"-table\" is not specified.", e.toString());
        }

        // test throwing PARAMETER_EMPTY "table"
        when(optionsHelper.hasOption(OPTION_TABLE)).thenReturn(true);
        when(optionsHelper.getOptionValue(OPTION_TABLE)).thenReturn(null);
        try {
            ReflectionTestUtils.invokeMethod(auditLogTool, "restore", optionsHelper, "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"table\" is empty.", e.toString());
        }
    }

    private void checkJsonl(long start, long end, File junitFolder) throws Exception {
        val name = String.format(Locale.ROOT, "%d-%d%s", start, end, AUDIT_LOG_SUFFIX);
        Assertions.assertThat(junitFolder.listFiles()).anyMatch(f -> f.getName().equals(name));
        val jsonl = Arrays.stream(junitFolder.listFiles()).filter(f -> f.getName().equals(name)).findFirst().get();
        Assertions.assertThat(jsonl.length()).isGreaterThan(0);

        val jdbcTemplate = getJdbcTemplate();
        long before = jdbcTemplate.queryForObject(String.format(Locale.ROOT,
                "select count(1) from test_audit_Log where meta_ts between %d and %d", start, end), Long.class);
        long after = fileLines(jsonl);
        Assertions.assertThat(after).isEqualTo(before);
    }

    private long fileLines(File f) throws IOException {
        long lines = 0L;
        try (InputStream in = new FileInputStream(f);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()))) {
            while (br.readLine() != null) {
                lines++;
            }
        }
        return lines;
    }

    private JdbcTemplate getJdbcTemplate() {
        val auditLogStore = (JdbcAuditLogStore) getStore().getAuditLogStore();
        return auditLogStore.getJdbcTemplate();
    }

    private void prepareData() throws Exception {
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        final List<RawResource> metadata = JsonUtil
                .readValue(Paths.get(DATA_DIR, "ke_metadata_test.json").toFile(), new TypeReference<List<JsonNode>>() {
                }).stream().map(x -> {
                    try {
                        return new RawResource(x.get("meta_table_key").asText(),
                                ByteSource.wrap(JsonUtil.writeValueAsBytes(x.get("meta_table_content"))),
                                x.get("meta_table_ts").asLong(), x.get("meta_table_mvcc").asLong());
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }).filter(Objects::nonNull).collect(toList());
        final List<AuditLog> auditLog = JsonUtil
                .readValue(Paths.get(DATA_DIR, "ke_metadata_test_audit_log.json").toFile(),
                        new TypeReference<List<JsonNode>>() {
                        })
                .stream().map(x -> {
                    try {
                        return new AuditLog(x.get("id").asLong(), x.get("meta_key").asText(),
                                ByteSource.wrap(JsonUtil.writeValueAsBytes(x.get("meta_content"))),
                                x.get("meta_ts").asLong(), x.get("meta_mvcc").asLong(), x.get("unit_id").asText(),
                                x.get("operator").asText(), "");
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }).filter(Objects::nonNull).collect(toList());

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(project).processor(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            metadata.forEach(x -> resourceStore.checkAndPutResource(x.getResPath(), x.getByteSource(), -1));
            return 0;
        }).maxRetry(1).build());
        val auditLogStore = (JdbcAuditLogStore) getStore().getAuditLogStore();
        auditLogStore.batchInsert(auditLog);
    }

    @Test
    public void testBatchInsertWithNull() {
        List<AuditLog> auditLogs = new ArrayList<>();
        AuditLog auditLog = new AuditLog();
        auditLog.setId(0);
        auditLog.setInstance(null);
        auditLog.setMvcc(null);
        auditLog.setOperator(null);
        auditLog.setResPath(null);
        auditLog.setByteSource(null);
        auditLog.setTimestamp(null);
        auditLog.setUnitId(null);
        auditLogs.add(auditLog);
        val auditLogStore = (JdbcAuditLogStore) getStore().getAuditLogStore();
        auditLogStore.batchInsert(auditLogs);
    }
}
