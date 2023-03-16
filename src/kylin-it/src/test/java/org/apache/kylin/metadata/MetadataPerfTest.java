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
package org.apache.kylin.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.tool.MetadataTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

@Slf4j
@RunWith(Parameterized.class)
@Ignore
public class MetadataPerfTest extends NLocalFileMetadataTestCase {

    private static final String INSERT_SQL = "insert into %s ( META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC) values (?, ?, ?, ?)";

    private static final String TEMPLATE_FOLDER = "project_0";

    private static final String TEMPLATE_UUID = "dc2efa94-76b5-4a82-b080-5c783ead85f8";

    private static final String TEMPLATE_EXEC_UUID = "d5a549fb-275f-4464-b2b0-a96c7cdadbe2";

    private static final int SEGMENT_SIZE = 100;

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][] { //
                { 1, 100 }, { 2, 100 }, { 5, 100 }, //
                //                { 10, 100 }, { 20, 100 }, { 50, 100 } //
        });
    }

    private final int projectSize;

    private final int modelSize;

    public MetadataPerfTest(int pSize, int mSize) {
        this.projectSize = pSize;
        this.modelSize = mSize;
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        val config = getTestConfig();
        config.setProperty("kylin.metadata.url", "kylin2_" + projectSize + "_" + modelSize
                + "@jdbc,url=jdbc:mysql://sandbox:3306/kylin?rewriteBatchedStatements=true");
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void backup() throws IOException {
        long start = System.currentTimeMillis();
        val file = Paths.get("meta_backups", projectSize + "", modelSize + "").toFile();
        FileUtils.forceMkdir(file);
        log.info("start backup for {}", getTestConfig().getMetadataUrl());
        log.info("backup dir is {}", file.getAbsolutePath());
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-backup", "-dir", file.getAbsolutePath() });
        log.info("backup finished for {}", getTestConfig().getMetadataUrl());
        long end = System.currentTimeMillis();
        log.info("usage time: {} seconds", (end - start) / 1000.0);
    }

    @Test
    public void restore() {
        long start = System.currentTimeMillis();
        val file = Paths.get("meta_backups", projectSize + "", modelSize + "").toFile();
        log.info("start restore for {}", getTestConfig().getMetadataUrl());
        log.info("restore dir is {}", file.getAbsolutePath());
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-dir", file.getAbsolutePath(), "--after-truncate" });
        log.info("restore finished for {}", getTestConfig().getMetadataUrl());
        long end = System.currentTimeMillis();
        log.info("usage time: {} seconds", (end - start) / 1000.0);
    }

    private static final String COUNT_ALL_SQL = "select count(1) from %s";
    private static final String SELECT_ALL_KEY_SQL = "select meta_table_key from %s where META_TABLE_KEY > '%s' order by META_TABLE_KEY limit %s";

    @Test
    public void loadIds() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        val table = getTestConfig().getMetadataUrl().getIdentifier();
        long count = jdbcTemplate.queryForObject(String.format(Locale.ROOT, COUNT_ALL_SQL, table), Long.class);
        long offset = 0;
        long pageSize = 1000;
        List<String> result = Lists.newArrayList();
        var prevKey = "/";
        while (offset < count) {
            for (String resource : jdbcTemplate.queryForList(
                    String.format(Locale.ROOT, SELECT_ALL_KEY_SQL, table, prevKey, pageSize), String.class)) {
                //                result.add(resource);
                log.debug("just print it {}", resource);
            }
            offset += pageSize;
        }
        log.info("all path size: {}", result.size());
    }

    @Test
    public void prepareData() throws Exception {
        val skip = Boolean.parseBoolean(System.getProperty("skipPrepare", "false"));
        if (skip) {
            return;
        }
        val jdbcTemplate = getJdbcTemplate();
        log.debug("drop table if exists");
        val table = getTestConfig().getMetadataUrl().getIdentifier();
        jdbcTemplate.update("drop table if exists " + table);
        val metaStore = MetadataStore.createMetadataStore(getTestConfig());
        log.debug("create a new table");
        val method = metaStore.getClass().getDeclaredMethod("createIfNotExist");
        Unsafe.changeAccessibleObject(method, true);
        method.invoke(metaStore);

        val START_ID = 1000;
        generateProject(START_ID, projectSize, TEMPLATE_FOLDER);

        val allIds = IntStream.range(START_ID, projectSize + START_ID).parallel().boxed()
                .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        val projectParams = IntStream.range(START_ID, projectSize + START_ID).mapToObj(i -> {
            val projectFile = new File(new File(TEMPLATE_FOLDER).getParentFile(),
                    "tmp_" + i + "/project_" + i + "/project.json");
            try {
                return new Object[] { "/_global/project/project_" + i,
                        IOUtils.toByteArray(new FileInputStream(projectFile)), projectFile.lastModified(), 0L };
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table), projectParams);
        Runnable run = () -> IntStream.range(START_ID, projectSize + START_ID).forEach(i -> {
            try {
                val dstFolder = new File(new File(TEMPLATE_FOLDER).getParentFile(), "tmp_" + i + "/project_" + i);
                val root = dstFolder.getParentFile();
                val files = FileUtils.listFiles(root, null, true);
                var sorted = Lists.newArrayList(files);
                sorted.sort(Comparator.comparing(f -> f.getPath().replace(root.getPath(), "")));
                log.info("start import to DB, all size is {}", sorted.size());
                var params = Lists.<Object[]> newArrayList();
                int index = 1;
                int[] argTypes = new int[] { Types.VARCHAR, Types.BINARY, Types.BIGINT, Types.BIGINT };
                for (File f : sorted) {
                    if (f.getName().startsWith(".")) {
                        continue;
                    }
                    try (val fis = new FileInputStream(f)) {
                        val resPath = f.getPath().replace(root.getPath(), "");
                        val bs = IOUtils.toByteArray(fis);
                        params.add(new Object[] { resPath, bs, f.lastModified(), 0L });
                    } catch (IOException e) {
                        throw new IllegalArgumentException("cannot not read file " + f, e);
                    }
                    if (index % 2000 == 0) {
                        log.debug("batch {} {}", index, params.size());
                        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table), params, argTypes);
                        params = Lists.newArrayList();
                    }
                    index++;
                }
                if (params.size() > 0) {
                    jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table), params);
                }
                allIds.remove(i);
                FileUtils.deleteQuietly(dstFolder.getParentFile());
            } catch (Exception e) {
                log.warn("some error", e);
            }
        });
        run.run();
        //        new ForkJoinPool(Math.min(projectSize, 30)).submit(run).join();
        log.debug("finish");
        if (!allIds.isEmpty()) {
            log.info("these are failed: {}", allIds);
            Assert.fail();
        }
    }

    private void generateProject(int startId, int size, String templateFolder) throws IOException {
        if (size < 1) {
            return;
        }
        val detailJobMap = Maps.<String, String> newHashMap();
        for (File file : FileUtils.listFiles(new File(TEMPLATE_FOLDER, "dataflow_details/" + TEMPLATE_UUID), null,
                false)) {
            if (file.getName().startsWith(".")) {
                continue;
            }
            val details = JsonUtil.readValue(file, NDataSegDetails.class);
            detailJobMap.put(details.getId(), details.getLayoutById(1).getBuildJobId());
        }
        val projectName = "project_" + startId;
        log.info("start generate data for {}", projectName);
        val dstFolder = new File(new File(templateFolder).getParentFile(), "tmp_" + startId + "/" + projectName);
        FileUtils.copyDirectory(new File(templateFolder), dstFolder,
                pathname -> !pathname.getPath().contains("execute"));
        for (int j = 1; j < modelSize; j++) {
            val newId = RandomUtil.randomUUIDStr();
            for (String sub : new String[] { "model_desc", "index_plan", "dataflow" }) {
                val file = new File(dstFolder, sub + "/" + TEMPLATE_UUID + ".json");
                val newFile = new File(dstFolder, sub + "/" + newId + ".json");
                FileUtils.copyFile(file, newFile);
                replaceInFile(newFile,
                        Arrays.asList(Pair.newPair(TEMPLATE_UUID, newId), Pair.newPair("model_0", "model_" + j)));
            }
            FileUtils.copyDirectory(new File(TEMPLATE_FOLDER, "dataflow_details/" + TEMPLATE_UUID),
                    new File(dstFolder, "dataflow_details/" + newId));
            val templateJobs = FileUtils.listFiles(new File(TEMPLATE_FOLDER, "execute"), null, false);
            val newJobsMap = templateJobs.stream().map(f -> Pair.newPair(f.getName(), RandomUtil.randomUUIDStr()))
                    .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            newJobsMap.forEach((k, v) -> {
                try {
                    val name = "execute/" + k;
                    val file = new File(TEMPLATE_FOLDER, name);
                    val newFile = new File(dstFolder, name.replace(k, v));
                    FileUtils.copyFile(file, newFile);
                    replaceInFile(newFile, Arrays.asList(Pair.newPair(TEMPLATE_UUID, newId), Pair.newPair(k, v),
                            Pair.newPair("project_0", projectName)));
                } catch (IOException ignore) {
                }
            });
            for (File file : FileUtils.listFiles(new File(dstFolder, "dataflow_details/" + newId), null, false)) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                val oldId = detailJobMap.get(file.getName().split("\\.")[0]);
                replaceInFile(file,
                        Arrays.asList(Pair.newPair(TEMPLATE_UUID, newId), Pair.newPair(oldId, newJobsMap.get(oldId))));
            }
        }

        for (int i = 1; i < size; i++) {
            val projectName2 = "project_" + (startId + i);
            val dstFolder2 = new File(new File(templateFolder).getParentFile(),
                    "tmp_" + (startId + i) + "/" + projectName2);
            if (dstFolder2.exists())
                continue;
            FileUtils.copyDirectory(dstFolder, dstFolder2);

            File projectJson = new File(dstFolder2 + File.separator, "project.json");
            var projectJsonContent = new String(Files.readAllBytes(projectJson.toPath()), StandardCharsets.UTF_8);
            projectJsonContent = projectJsonContent.replaceAll("958983a5-fad8-4057-9d70-cd6e5a2374af",
                    RandomUtil.randomUUIDStr());
            Files.write(projectJson.toPath(), projectJsonContent.getBytes(StandardCharsets.UTF_8));

            val sub = "execute";
            for (File file : FileUtils.listFiles(new File(dstFolder, sub), null, true)) {
                var content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                content = content.replaceAll("project_0", projectName2);
                Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private void replaceInFile(File file, List<Pair<String, String>> pairs) throws IOException {
        var content = FileUtils.readFileToString(file);
        for (Pair<String, String> pair : pairs) {
            content = content.replaceAll(pair.getFirst(), pair.getSecond());
        }
        FileUtils.writeStringToFile(file, content, false);
    }

    private JdbcTemplate getJdbcTemplate() throws Exception {
        val metaStore = MetadataStore.createMetadataStore(getTestConfig());
        val field = metaStore.getClass().getDeclaredField("jdbcTemplate");
        Unsafe.changeAccessibleObject(field, true);
        return (JdbcTemplate) field.get(metaStore);
    }

    private DataSourceTransactionManager getTransactionManager() throws Exception {
        val metaStore = MetadataStore.createMetadataStore(getTestConfig());
        val field = metaStore.getClass().getDeclaredField("transactionManager");
        Unsafe.changeAccessibleObject(field, true);
        return (DataSourceTransactionManager) field.get(metaStore);
    }

    @Test
    public void prepareTemplate() throws IOException {
        val indexFile = new File(TEMPLATE_FOLDER, "index_plan/" + TEMPLATE_UUID + ".json");
        val indexPlan = JsonUtil.readValue(indexFile, IndexPlan.class);
        List<Long> layoutIds = Lists.newArrayList();
        List<IndexEntity> indexes = Stream
                .of(Tuple3.apply(Lists.newArrayList(1, 2, 3, 4), Lists.newArrayList(NDataModel.MEASURE_ID_BASE), 0L),
                        Tuple3.apply(Lists.newArrayList(5, 6, 7, 8),
                                Lists.newArrayList(NDataModel.MEASURE_ID_BASE + 1, NDataModel.MEASURE_ID_BASE + 2),
                                IndexEntity.INDEX_ID_STEP),
                        Tuple3.apply(Lists.newArrayList(1, 2), Lists.newArrayList(NDataModel.MEASURE_ID_BASE),
                                IndexEntity.TABLE_INDEX_START_ID))
                .map(t -> {
                    val index1 = new IndexEntity();
                    index1.setId(t._3());
                    index1.setDimensions(t._1());
                    index1.setMeasures(t._2());
                    val id = new AtomicLong(t._3() + 1);
                    index1.setLayouts(permutation(index1.getDimensions()).stream().map(ds -> {
                        val entity = new LayoutEntity();
                        entity.setId(id.getAndIncrement());
                        ds.addAll(index1.getMeasures());
                        entity.setColOrder(ds);
                        entity.setAuto(true);
                        layoutIds.add(entity.getId());
                        return entity;
                    }).collect(Collectors.toList()));
                    return index1;
                }).collect(Collectors.toList());
        indexPlan.setIndexes(indexes);
        JsonUtil.writeValueIndent(new FileOutputStream(indexFile), indexPlan);

        val dfFile = new File(TEMPLATE_FOLDER, "dataflow/" + TEMPLATE_UUID + ".json");
        val df = JsonUtil.readValue(dfFile, NDataflow.class);
        val segments = new Segments<NDataSegment>();
        for (int i = 0; i < SEGMENT_SIZE; i++) {
            val start = LocalDate.parse("2000-01-01").plusMonths(i);
            val end = start.plusMonths(1);
            val seg = NDataSegment.empty();
            val segRange = new SegmentRange.TimePartitionedSegmentRange(start.toString(), end.toString());
            seg.setId(RandomUtil.randomUUIDStr());
            seg.setName(Segments.makeSegmentName(segRange));
            seg.setCreateTimeUTC(System.currentTimeMillis());
            seg.setSegmentRange(segRange);
            seg.setStatus(SegmentStatusEnum.READY);
            segments.add(seg);
        }
        df.setSegments(segments);
        JsonUtil.writeValueIndent(new FileOutputStream(dfFile), df);

        val detailJobMap = Maps.<String, String> newHashMap();
        for (NDataSegment segment : segments) {
            val detailFile = new File(TEMPLATE_FOLDER,
                    "dataflow_details/" + TEMPLATE_UUID + "/" + segment.getId() + ".json");
            detailJobMap.put(segment.getId(), RandomUtil.randomUUIDStr());
            val detail = new NDataSegDetails();
            detail.setUuid(segment.getId());
            detail.setDataflowId(df.getUuid());
            detail.setLayouts(layoutIds.stream().map(id -> {
                val layout = new NDataLayout();
                layout.setLayoutId(id);
                layout.setBuildJobId(detailJobMap.get(segment.getId()));
                layout.setByteSize(1000);
                layout.setFileCount(1);
                layout.setRows(1024);
                layout.setSourceRows(1025);
                layout.setSourceByteSize(0);
                return layout;
            }).collect(Collectors.toList()));
            JsonUtil.writeValueIndent(new FileOutputStream(detailFile), detail);
        }

        for (Map.Entry<String, String> entry : detailJobMap.entrySet()) {
            val newExecId = entry.getValue();
            val name = "execute/" + TEMPLATE_EXEC_UUID;
            val file = new File(TEMPLATE_FOLDER, name);
            val newFile = new File(TEMPLATE_FOLDER, name.replace(TEMPLATE_EXEC_UUID, newExecId));
            FileUtils.copyFile(file, newFile);
            replaceInFile(newFile,
                    Arrays.asList(Pair.newPair("1,20001,10001", Joiner.on(",").join(layoutIds)),
                            Pair.newPair(TEMPLATE_EXEC_UUID, newExecId),
                            Pair.newPair("facd8577-8fca-48f5-803c-800ea75c8495", entry.getKey())));
        }

    }

    public static List<List<Integer>> permutation(List<Integer> s) {
        List<List<Integer>> res = new ArrayList<>();
        if (s.size() == 1) {
            res.add(s);
        } else if (s.size() > 1) {
            int lastIndex = s.size() - 1;
            List<Integer> last = s.subList(lastIndex, lastIndex + 1);
            List<Integer> rest = s.subList(0, lastIndex);
            res = merge(permutation(rest), last);
        }
        return res;
    }

    public static List<List<Integer>> merge(List<List<Integer>> list, List<Integer> c) {
        ArrayList<List<Integer>> res = new ArrayList<>();
        for (List<Integer> s : list) {
            for (int i = 0; i <= s.size(); ++i) {
                List<Integer> ps = Lists.newArrayList(s);
                ps.addAll(i, c);
                res.add(ps);
            }
        }
        return res;
    }

}
