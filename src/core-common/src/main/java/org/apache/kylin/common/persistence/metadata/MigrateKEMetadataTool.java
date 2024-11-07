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

import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_IMAGE_META_KEY_TAG;
import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_UUID_META_KEY_TAG;
import static org.apache.kylin.common.persistence.ResourceStore.REC_FILE;
import static org.apache.kylin.common.persistence.ResourceStore.VERSION_FILE_META_KEY_TAG;
import static org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore.Type.DIR;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.persistence.resources.CcModelRelationRawResource;
import org.apache.kylin.common.persistence.resources.ComputeColumnRawResource;
import org.apache.kylin.common.persistence.resources.DataParserRawResource;
import org.apache.kylin.common.persistence.resources.LayoutRawResource;
import org.apache.kylin.common.persistence.resources.RecRawResource;
import org.apache.kylin.common.persistence.resources.SegmentRawResource;
import org.apache.kylin.common.persistence.resources.SegmentRawResourceWrap;
import org.apache.kylin.common.persistence.resources.SystemRawResource;
import org.apache.kylin.common.persistence.resources.TableModelRelationRawResource;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.tool.util.HashFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

/**
 * 02/01/2024 hellozepp(lisheng.zhanglin@163.com)
 */
@Slf4j
public class MigrateKEMetadataTool {

    public static final String ZIP_SUFFIX = ".zip";
    public static final String PROJECT_KEY = "project";
    public final Map<String, Map<String, String>> modelUuidMap = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> segUuidMap = new ConcurrentHashMap<>();

    private final Map<String, String> ccUuidMap = new ConcurrentHashMap<>();

    public static final Serializer<StringEntity> serializer = new Serializer<StringEntity>() {
        @Override
        public void serialize(StringEntity obj, DataOutputStream out) throws IOException {
            out.writeUTF(obj.getStr());
        }

        @Override
        public StringEntity deserialize(DataInputStream in) throws IOException {
            String str = in.readUTF();
            return new StringEntity(str);
        }
    };

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Usage: MigrateKEMetadataTool <inputPath> <outputPath>");
            return;
        }
        String inputPath = args[0];
        String outputPath = null;
        if (args.length > 1) {
            outputPath = args[1];
        }
        val tool = new MigrateKEMetadataTool();
        tool.doMigrate(inputPath, outputPath);
    }

    public MetadataStore.MemoryMetaData loadOldMetaData(FileSystemMetadataStore metadataStore, String inputPath,
                                                        String type) {
        Path path = new Path(inputPath);

        if (DIR.name().equals(type)) {
            return getAllFile(path, metadataStore);
        }
        return fillMetaDataFromCompressedFile(path, metadataStore);
    }

    private MetadataStore.MemoryMetaData getAllFile(Path filePath, FileSystemMetadataStore store) {
        Date startTime = new Date();
        val fs = store.getFs();
        MetadataStore.MemoryMetaData data = MetadataStore.MemoryMetaData.createEmpty();

        // Extract duplicate code, load data to MemoryMetaData through FileStatus
        BiConsumer<FileStatus, MetadataStore.MemoryMetaData> loadMetadataProcess = (innerStat, dataHelper) -> {
            try {
                val innerRaw = loadOne(innerStat, dataHelper, store, filePath);
                if (innerRaw != null) {
                    dataHelper.put(innerRaw.getMetaType(), new VersionedRawResource(innerRaw));
                }
            } catch (IOException e) {
                Throwables.throwIfUnchecked(e);
            }
        };

        try {
            FileStatus[] fileStatuses = fs.listStatus(filePath);
            log.info("getAllFile from {} started...", filePath);
            List<Future<?>> futures = new ArrayList<>();
            for (FileStatus childStatus : fileStatuses) {
                store.getAndPutAllFileRecursion(childStatus, fs, data, futures, loadMetadataProcess);
            }

            // wait for fileSystemMetadataExecutor to finish
            for (Future<?> future : futures) {
                future.get();
            }

            log.info("getAllFile cost {} ms", new Date().getTime() - startTime.getTime());
            return data;
        } catch (IOException | ExecutionException e) {
            throw new KylinRuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KylinRuntimeException(e);
        }
    }

    private MetadataStore.MemoryMetaData fillMetaDataFromCompressedFile(Path compressedFile,
            FileSystemMetadataStore store) {
        log.info("reloadAll from zip");
        val fs = store.getFs();
        MetadataStore.MemoryMetaData data = MetadataStore.MemoryMetaData.createEmpty();
        try {
            if (!fs.exists(compressedFile) || !fs.isFile(compressedFile)) {
                return data;
            }
        } catch (IOException ignored) {
            log.warn("Check zip file failed, return empty.");
            return data;
        }

        try (FSDataInputStream in = fs.open(compressedFile); ZipInputStream zipIn = new ZipInputStream(in)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                String resPath = zipEntry.getName();
                if (verifyNonMetadataFile(resPath)) {
                    log.info("not a metadata file: " + resPath + ", skip it!");
                    continue;
                }
                long t = zipEntry.getTime();
                val raw = loadByStream(resPath, t, data, new DataInputStream(zipIn));
                if (raw != null) {
                    data.put(raw.getMetaType(), new VersionedRawResource(raw));
                }
            }
        } catch (Exception e) {
            log.warn("get file from compressed file error", e);
        }
        return data;
    }

    private RawResource loadOne(FileStatus status, MetadataStore.MemoryMetaData data,
            FileSystemMetadataStore metadataStore, Path parentPath) throws IOException {
        Path curPath = status.getPath();
        FileSystem fs = metadataStore.getFs();
        long ts = fs.getFileStatus(curPath).getModificationTime();

        // if you set kylin.env.engine-write-fs, the schema may be inconsistent.
        val replacedPath = Path.getPathWithoutSchemeAndAuthority(parentPath);
        val replacedValue = fs.makeQualified(replacedPath).toString();
        String resourcePath = curPath.toString().replace(replacedValue, "");
        if (verifyNonMetadataFile(resourcePath)) {
            return null;
        }
        try (val in = fs.open(curPath)) {
            return loadByStream(resourcePath, ts, data, new DataInputStream(in));
        }
    }

    private RawResource loadByStream(String resourcePath, long ts, MetadataStore.MemoryMetaData data, DataInputStream in)
            throws IOException {
        if (in.available() == 0) {
            return null;
        }

        MigrateRawResourceFactory factory = new MigrateRawResourceFactory();
        if (resourcePath.endsWith(METASTORE_UUID_META_KEY_TAG)) {
            return factory.createSystemRawResource(METASTORE_UUID_META_KEY_TAG, ts, in);
        } else if (resourcePath.endsWith(VERSION_FILE_META_KEY_TAG)) {
            return factory.createSystemRawResource(VERSION_FILE_META_KEY_TAG, ts, in);
        } else if (resourcePath.endsWith(METASTORE_IMAGE_META_KEY_TAG)) {
            return factory.createSystemRawResource(METASTORE_IMAGE_META_KEY_TAG, ts, in);
        } else if (resourcePath.contains(ResourceStore.METASTORE_TRASH_RECORD_KEY)) {
            return null;
        } else if (resourcePath.contains("/" + REC_FILE + "/")) {
            return factory.createRecResource(resourcePath, ts, in);
        } else {
            byte[] byteArray = IOUtils.toByteArray(in);
            RawResource res;
            val resPathPair = splitFilePath(resourcePath);
            switch (resPathPair._2()) {
            case DATAFLOW:
                res = factory.createDataflowResource(resPathPair, ts, byteArray, data);
                break;
            case MODEL:
                res = factory.createModelResource(resPathPair, ts, byteArray, data);
                break;
            case LAYOUT:
                res = factory.createLayoutResource(resPathPair, byteArray);
                break;
            case DATA_PARSER:
                res = factory.createDataParserResource(resPathPair, byteArray);
                break;
            case INDEX_PLAN:
                res = factory.createIndexPlanResource(resPathPair, byteArray);
                break;
            default:
                res = factory.createDefaultResource(resPathPair, byteArray);
            }

            if (res.getMetaKey() == null) {
                res.setMetaKey(generateMetaKey(resPathPair, res));
            }

            res.setMvcc(0);
            res.setTs(ts);
            return res;
        }
    }

    private static String generateMetaKey(Tuple3<String, MetadataType, String> resPathPair, RawResource res) {
        String resourceName = resPathPair._3().endsWith(".json")
                ? resPathPair._3().substring(0, resPathPair._3().length() - 5)
                : resPathPair._3();
        if (resPathPair._2() == MetadataType.TABLE_EXD || resPathPair._2() == MetadataType.TABLE_INFO
                || resPathPair._2() == MetadataType.KAFKA_CONFIG || resPathPair._2() == MetadataType.JAR_INFO) {
            return res.getProject() + "." + resourceName;
        } else {
            return resourceName;
        }
    }

    /**
     * Split resource path to project, type and key
     * @param resourcePath The resource path
     * @return The tuple of project, type and metaKey
     */
    public static Tuple3<String, MetadataType, String> splitFilePath(String resourcePath) {
        if ("/".equals(resourcePath)) {
            return new Tuple3<>(resourcePath, MetadataType.ALL, null);
        } else if (resourcePath.startsWith("/") && resourcePath.length() > 1) {
            resourcePath = resourcePath.substring(1);
        }

        if (resourcePath.contains("_global/sys_acl/user")) {
            String[] split = resourcePath.split("/");
            return new Tuple3<>(split[0], MetadataType.USER_GLOBAL_ACL, split[3]);
        } else if (resourcePath.contains("_global/acl")) {
            String[] split = resourcePath.split("/");
            return new Tuple3<>(split[0], MetadataType.OBJECT_ACL, split[2]);
        } else if (resourcePath.contains("/acl/user")) {
            String[] split = resourcePath.split("/");
            return new Tuple3<>(split[0], MetadataType.ACL, split[0] + "." + "u" + "." + split[3]);
        } else if (resourcePath.contains("/acl/group")) {
            String[] split = resourcePath.split("/");
            return new Tuple3<>(split[0], MetadataType.ACL, split[0] + "." + "g" + "." + split[3]);
        }
        String[] split = resourcePath.split("/", 3);
        if (split.length < 3) {
            throw new KylinRuntimeException("resourcePath is invalid: " + resourcePath);
        }
        String typeStr = convertToMigrateMetadataType(split[1].toUpperCase(Locale.ROOT));
        return new Tuple3<>(split[0], MetadataType.create(typeStr), split[2]);
    }

    private static String convertToMigrateMetadataType(String type) {
        switch (type) {
        case "DATAFLOW_DETAILS":
            type = "LAYOUT";
            break;
        case "MODEL_DESC":
            type = "MODEL";
            break;
        case "TABLE":
        case "USER":
        case "JAR":
            type += "_INFO";
            break;
        case "KAFKA":
            type = "KAFKA_CONFIG";
            break;
        case "STREAMING":
            type = "STREAMING_JOB";
            break;
        case "QUERY":
            type = "QUERY_RECORD";
            break;
        case "PARSER":
            type = "DATA_PARSER";
            break;
        default:
            break;
        }
        return type;
    }

    public static void migrateMetaFromJsonList(String inputFile, String outFile, boolean isAuditLog) throws Exception {
        String metaKey = isAuditLog ? "meta_key" : "meta_table_key";
        String metaContent = isAuditLog ? "meta_content" : "meta_table_content";
        String metaTs = isAuditLog ? "meta_ts" : "meta_table_ts";

        MigrateKEMetadataTool tool = new MigrateKEMetadataTool();
        MetadataStore.MemoryMetaData data = MetadataStore.MemoryMetaData.createEmpty();
        List<JsonNode> items = JsonUtil.readValue(Paths.get(inputFile).toFile(), new TypeReference<List<JsonNode>>() {
        });
        List<ObjectNode> result = new ArrayList<>();
        for (JsonNode item : items) {
            String originMetaKey = item.get(metaKey).asText();
            long originMetaTs = item.get(metaTs).asLong();
            byte[] originContent = JsonUtil.writeValueAsBytes(item.get(metaContent));
            try (ByteArrayInputStream bis = new ByteArrayInputStream(originContent);
                    DataInputStream in = new DataInputStream(bis)) {
                RawResource raw = tool.loadByStream(originMetaKey, originMetaTs, data, in);
                if (raw != null) {
                    ObjectNode node = JsonUtil.valueToTree(item);
                    node.put(metaKey, raw.generateKeyWithType());
                    node.set(metaContent, JsonUtil.readValue(raw.getContent(), JsonNode.class));
                    result.add(node);
                }
            } catch (Exception e) {
                log.warn("Migrate failed for metadata: {}, just ignore it. ", originMetaKey, e);
            }
        }
        File output = Paths.get(outFile).toFile();
        if (!output.getParentFile().exists()) {
            output.getParentFile().mkdirs();
        }
        JsonUtil.writeValueIndent(Files.newOutputStream(output.toPath()), result);
    }

    public static void migrateUtMetaZipFiles(String inputPath, String outputPath) throws Exception {
        KylinConfig inputConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConf = KapConfig.wrap(inputConfig);
        if (inputPath != null) {
            inputPath = StringUtils.appendIfMissing(inputPath, "/");
        } else {
            inputPath = inputConfig.getMetadataUrl().getIdentifier();
        }

        if (outputPath == null) {
            outputPath = kapConf.getReadHdfsWorkingDirectory() + "migrated/";
        }

        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        log.info("ut-meta metadataPath: " + outputPath);
        String finalOutputPath = outputPath;
        Path inputFilePath = new Path(inputPath);
        TreeSet<Path> allFilePath = FileSystemMetadataStore.getAllFilePath(inputFilePath, fs);
        allFilePath.forEach(path -> {
            if (!path.getName().endsWith(".zip")) {
                log.info("skip file: {}", path);
                return;
            }
            long date = System.currentTimeMillis();
            log.info("start migrate file: {}", path);
            try {
                val replacedPath = Path.getPathWithoutSchemeAndAuthority(inputFilePath);
                val replacedValue = fs.makeQualified(replacedPath).toString();
                val migrateKEMetadataTool = new MigrateKEMetadataTool();
                migrateKEMetadataTool.doMigrate(path.toString(),
                        path.toString().replace(replacedValue, finalOutputPath));
                log.info("end migrate file: {}, cost: {}", path, System.currentTimeMillis() - date);
            } catch (Exception e) {
                throw new KylinRuntimeException(e);
            }

        });
        fixSignatureForZipFiles(outputPath);
    }

    /**
     * Limitation: The outputPath directory only contains zip packages
     * @param outputPath
     */
    private static void fixSignatureForZipFiles(String outputPath) {
        java.io.File folder = new java.io.File(outputPath);
        Arrays.stream(folder.listFiles()).forEach(file -> {
            try {
                if (!file.getName().endsWith(".zip")) {
                    return;
                }
                if (file.getName().length() - file.getName().lastIndexOf("_") != 37) {
                    return;
                }
                InputStream is = Files.newInputStream(file.toPath());
                byte[] md5 = HashFunction.MD5.checksum(is);
                String signature = DatatypeConverter.printHexBinary(md5);
                String newFileName = file.getName().substring(0, file.getName().length() - 36) + signature + ZIP_SUFFIX;
                Files.move(file.toPath(), new java.io.File(outputPath, newFileName).toPath());
            } catch (IOException e) {
                throw new KylinRuntimeException(e);
            }
        });
    }

    public void doMigrate(String inputPath, String outputPath) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        KapConfig kapConf = KapConfig.wrap(config);
        if (inputPath == null) {
            inputPath = config.getMetadataUrl().getIdentifier();
        }
        final Properties props = config.exportToProperties();
        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);

        String type = inputPath.endsWith(".zip") ? FileSystemMetadataStore.Type.ZIP.name() : DIR.name();
        if (outputPath == null) {
            if (type.equals("ZIP")) {
                outputPath = inputPath.replace(".zip", "_migrated.zip");
            } else {
                outputPath = kapConf.getReadHdfsWorkingDirectory() + "migrated/";
            }
            log.info("outputPath is not set, use default: " + outputPath);
        } else {
            log.info("outputPath: " + outputPath);
        }

        String outputZipFile = null;
        if (outputPath.endsWith(".zip")) {
            // metnauadata_url doesn't support zip file
            outputZipFile = outputPath;
            outputPath = outputPath.substring(0, outputPath.lastIndexOf("/"));
        }

        Path metadataPath = new Path(outputPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem(metadataPath);
        if (!fs.exists(metadataPath)) {
            fs.mkdirs(metadataPath);
        }
        if (type.equals(FileSystemMetadataStore.Type.ZIP.name())) {
            dstConfig.setMetadataUrl(outputPath + "@file,zip=1");
        } else {
            dstConfig.setMetadataUrl(outputPath);
        }
        val dstResourceStore = (InMemResourceStore) ResourceStore.getKylinMetaStore(dstConfig);

        FileSystemMetadataStore outputMetadataStore = (FileSystemMetadataStore) MetadataStore
                .createMetadataStore(dstConfig);
        MetadataStore.MemoryMetaData memoryMetaData = loadOldMetaData(outputMetadataStore, inputPath, type);
        dstResourceStore.resetData(memoryMetaData);
        dumpToNewFormat(outputMetadataStore, dstResourceStore, type, outputZipFile);
    }

    private void dumpToNewFormat(FileSystemMetadataStore fileStore, ResourceStore resourceStore, String type,
            String outputZipFile) throws IOException, InterruptedException, ExecutionException {
        val resources = resourceStore.listResourcesRecursively(MetadataType.ALL.name());
        if (FileSystemMetadataStore.Type.ZIP.name().equals(type)) {
            fileStore.dumpToZip(resourceStore, resources, new Path(outputZipFile));
        } else {
            fileStore.dumpToFile(resourceStore, resources);
        }
    }

    /**
     *
     * @param globalMap {@link MigrateKEMetadataTool#modelUuidMap} or {@link MigrateKEMetadataTool#segUuidMap}
     * @param originUuid modelUuid or segmentUuid
     * @param scope when originUuid is modelUuid, scope is project; when originUuid is segmentUuid, scope is model_uuid.
     * @return a unique modelUuid or segmentUuid
     */
    public static String getUniqueUuid(Map<String, Map<String, String>> globalMap,
                                       String scope, String originUuid) {
        Map<String, String> projectMap = globalMap.computeIfAbsent(originUuid,
                k -> new ConcurrentHashMap<>());
        if (projectMap.get(scope) == null) {
            synchronized (projectMap) {
                if (projectMap.get(scope) == null) {
                    projectMap.put(scope, projectMap.isEmpty() ? originUuid : RandomUtil.randomUUIDStr());
                    if (!originUuid.equals(projectMap.get(scope))) {
                        log.info("Rename metadata uuid from {} to {} in scope {}", originUuid, projectMap.get(scope),
                                scope);
                    }
                }
            }
        }
        return projectMap.get(scope);
    }

    private class MigrateRawResourceFactory {
        private void createAndSaveCcRelations(JsonNode je, ObjectNode modelMap, String proj, String modelUuid,
                long ts, MetadataStore.MemoryMetaData data, ArrayNode ccUuids) throws IOException {
            if (je.has("computed_columns")) {
                Object s = modelMap.get("computed_columns");
                JsonNode entries = JsonUtil.readValue(JsonUtil.writeValueAsIndentBytes(s), ArrayNode.class);
                for (JsonNode entry : entries) {
                    String ccUuid = checkAndCreateCC(entry, proj, ts, data);
                    ccUuids.add(ccUuid);
                    CcModelRelationRawResource ccRel = createCCRelation(ccUuid, proj, modelUuid, ts);
                    data.put(MetadataType.CC_MODEL_RELATION, new VersionedRawResource(ccRel));
                }
                modelMap.remove("computed_columns");
            }
        }

        private String checkAndCreateCC(JsonNode entry, String proj, long ts, MetadataStore.MemoryMetaData data)
                throws IOException {
            LinkedHashMap<String, String> contentJsonMap = JsonUtil.convert(entry,
                    new TypeReference<LinkedHashMap<String, String>>() {
                    });
            String uniqueKey = proj + "." + contentJsonMap.get("tableIdentity") + "."
                    + contentJsonMap.get("columnName");
            String ccUuid = ccUuidMap.get(uniqueKey);
            boolean needCreate = false;
            if (ccUuid == null) {
                synchronized (ccUuidMap) {
                    ccUuid = ccUuidMap.get(uniqueKey);
                    if (ccUuid == null) {
                        needCreate = true;
                        ccUuid = contentJsonMap.containsKey("rec_uuid") && contentJsonMap.get("rec_uuid") != null
                                ? contentJsonMap.get("rec_uuid")
                                : RandomUtil.randomUUIDStr();
                        ccUuidMap.put(uniqueKey, ccUuid);
                    }
                }
            }
            if (needCreate) {
                contentJsonMap.put("uuid", ccUuid);
                contentJsonMap.put(PROJECT_KEY, proj);
                ComputeColumnRawResource cc = JsonUtil.convert(entry, ComputeColumnRawResource.class);
                cc.setUuid(ccUuid);
                cc.setProject(proj);
                cc.setContent(JsonUtil.writeValueAsIndentBytes(contentJsonMap));
                cc.setMetaKey(ccUuid);
                cc.setMvcc(0);
                cc.setTs(ts);
                data.put(MetadataType.COMPUTE_COLUMN, new VersionedRawResource(cc));
            }
            return ccUuid;
        }

        private CcModelRelationRawResource createCCRelation(String ccUuid, String proj, String modelUuid,
                long ts) throws JsonProcessingException {
            CcModelRelationRawResource ccRel = new CcModelRelationRawResource();
            String ccRelUuid = RandomUtil.randomUUIDStr();
            ccRel.setUuid(ccRelUuid);
            ccRel.setMetaKey(ccRelUuid);
            ccRel.setProject(proj);
            ccRel.setModelUuid(modelUuid);
            ccRel.setCcUuid(ccUuid);
            ccRel.setMvcc(0);
            Map<String, String> ccRelMap = new HashMap<>();
            ccRelMap.put("uuid", ccRelUuid);
            ccRelMap.put(PROJECT_KEY, proj);
            ccRelMap.put("model_uuid", modelUuid);
            ccRelMap.put("cc_uuid", ccUuid);
            ccRel.setContent(JsonUtil.writeValueAsIndentBytes(ccRelMap));
            ccRel.setTs(ts);
            return ccRel;
        }

        private void createAndSaveTableRelations(JsonNode je, ObjectNode modelMap, String proj, String modelUuid,
                long ts, MetadataStore.MemoryMetaData data) throws IOException {
            HashSet<String> refTables = new HashSet<>();
            if (je.has("join_tables")) {
                Object s = modelMap.get("join_tables");
                JsonNode entries = JsonUtil.readValue(JsonUtil.writeValueAsIndentBytes(s), ArrayNode.class);

                for (JsonNode entry : entries) {
                    String identifier = entry.get("table").asText();
                    if (refTables.contains(identifier)) {
                        continue;
                    }
                    refTables.add(identifier);
                    TableModelRelationRawResource relation = createTableModelRelation(modelUuid, proj, identifier, ts);
                    data.put(MetadataType.TABLE_MODEL_RELATION, new VersionedRawResource(relation));
                }
            }
            String factTable = je.get("fact_table").asText();
            if (!refTables.contains(factTable)) {
                TableModelRelationRawResource relation = createTableModelRelation(modelUuid, proj, factTable, ts);
                data.put(MetadataType.TABLE_MODEL_RELATION, new VersionedRawResource(relation));
            }
        }

        private TableModelRelationRawResource createTableModelRelation(String modelUuid, String proj,
                String tableIdentity, long ts) throws JsonProcessingException {
            String uuid = RandomUtil.randomUUIDStr();
            TableModelRelationRawResource relation = new TableModelRelationRawResource();
            relation.setUuid(uuid);
            relation.setModelUuid(modelUuid);
            relation.setProject(proj);
            relation.setTableIdentity(tableIdentity);
            Map<String, String> map = new HashMap<>();
            map.put("uuid", uuid);
            map.put("model_uuid", modelUuid);
            map.put(PROJECT_KEY, proj);
            map.put("table_identity", tableIdentity);
            relation.setContent(JsonUtil.writeValueAsIndentBytes(map));
            relation.setMetaKey(uuid);
            relation.setMvcc(0);
            relation.setTs(ts);
            return relation;
        }

        public RawResource createSystemRawResource(String tagName, long ts, DataInputStream in) throws IOException {
            SystemRawResource bs = new SystemRawResource();
            byte[] content = null;
            switch (tagName) {
            case METASTORE_UUID_META_KEY_TAG:
                StringEntity deserialize;
                try {
                    deserialize = serializer.deserialize(in);
                } catch (Exception e) {
                    log.warn("loadZip error", e);
                    // Adapt to the situation where the uuid does not exist or the format is illegal
                    deserialize = new StringEntity(RandomUtil.randomUUIDStr());
                }
                deserialize.setName(METASTORE_UUID_META_KEY_TAG);
                deserialize.setUuid(deserialize.getStr());
                bs.setUuid(deserialize.getStr());
                content = JsonUtil.writeValueAsIndentBytes(deserialize);
                break;
            case METASTORE_IMAGE_META_KEY_TAG:
                ImageDesc image = JsonUtil.readValue(in, ImageDesc.class);
                content = JsonUtil.writeValueAsIndentBytes(image);
                break;
            case VERSION_FILE_META_KEY_TAG:
                String version = IOUtils.toString(in, StandardCharsets.UTF_8).trim();
                StringEntity versionEntity = new StringEntity(VERSION_FILE_META_KEY_TAG, version);
                content = JsonUtil.writeValueAsIndentBytes(versionEntity);
                break;
            default:
                break;
            }

            bs.setName(tagName);
            bs.setMetaKey(tagName);
            bs.setMvcc(0);
            bs.setTs(ts);
            bs.setContent(content);
            return bs;
        }

        public RawResource createRecResource(String resPath, long ts, DataInputStream in) throws IOException {
            if (resPath.startsWith("/")) {
                resPath = resPath.substring(1);
            }
            String[] split = resPath.split("/");
            String project = split[0];
            String uuid = split[2].replace(FileSystemMetadataStore.JSON_SUFFIX, "");
            byte[] byteArray = IOUtils.toByteArray(in);

            JsonNode recItems = JsonUtil.readValue(byteArray, JsonNode.class);
            ObjectNode recEntity = new ObjectNode(JsonNodeFactory.instance);
            recEntity.put("uuid", uuid);
            recEntity.put(PROJECT_KEY, project);
            recEntity.set("rec_items", recItems);

            RecRawResource bs = new RecRawResource();
            bs.setProject(project);
            bs.setUuid(RandomUtil.randomUUIDStr());
            bs.setContent(JsonUtil.writeValueAsIndentBytes(recEntity));
            bs.setMetaKey(project + "." + uuid);
            bs.setMvcc(0);
            bs.setTs(ts);
            return bs;
        }

        private RawResource createDataflowResource(Tuple3<String, MetadataType, String> resPathPair, long ts,
                byte[] byteArray, MetadataStore.MemoryMetaData data) throws IOException {
            JsonNode je = JsonUtil.readValue(byteArray, JsonNode.class);
            ObjectNode contentJsonMap = JsonUtil.valueToTree(je);
            String modelUuid = contentJsonMap.get("uuid").asText();
            String proj = resPathPair._1();
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                String uniqueUuid = getUniqueUuid(modelUuidMap, proj, modelUuid);
                if (!modelUuid.equals(uniqueUuid)) {
                    modelUuid = uniqueUuid;
                    contentJsonMap.put("uuid", modelUuid);
                    contentJsonMap.put("meta_key", modelUuid);
                }
            }
            ArrayNode segmentUuids = JsonUtil.createArrayNode();
            if (je.has("segments")) {
                JsonNode entries = contentJsonMap.get("segments");
                for (JsonNode entry : entries) {
                    val segmentWrap = JsonUtil.convert(entry, SegmentRawResourceWrap.class);
                    String uuid = segmentWrap.getId();
                    if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                        String uniqueUuid = getUniqueUuid(segUuidMap, modelUuid, uuid);
                        if (!uuid.equals(uniqueUuid)) {
                            uuid = uniqueUuid;
                        }
                    }
                    segmentWrap.setId(null);
                    segmentWrap.setUuid(uuid);
                    byte[] wrapJson = JsonUtil.writeValueAsIndentBytes(segmentWrap);
                    SegmentRawResource seg = JsonUtil.readValue(wrapJson, SegmentRawResource.class);
                    seg.setModelUuid(modelUuid);
                    seg.setProject(proj);
                    seg.setUuid(uuid);
                    LinkedHashMap<Object, Object> segContentMap = JsonUtil.convert(entry,
                            new TypeReference<LinkedHashMap<Object, Object>>() {
                            });
                    segContentMap.remove("id");
                    segContentMap.put("uuid", uuid);
                    segContentMap.put("model_uuid", modelUuid);
                    segContentMap.put(PROJECT_KEY, proj);

                    seg.setContent(JsonUtil.writeValueAsIndentBytes(segContentMap));
                    seg.setMetaKey(uuid);
                    seg.setMvcc(0);
                    seg.setTs(ts);
                    data.put(MetadataType.SEGMENT, new VersionedRawResource(seg));
                    segmentUuids.add(uuid);
                }
                contentJsonMap.remove("segments");
            }

            contentJsonMap.put(PROJECT_KEY, proj);
            contentJsonMap.set("segment_uuids", segmentUuids);
            byte[] contentByte = JsonUtil.writeValueAsIndentBytes(contentJsonMap);
            RawResource res = JsonUtil.readValue(contentByte, MetadataType.DATAFLOW.getResourceClass());
            res.setProject(proj);
            res.setContent(contentByte);
            return res;
        }

        private RawResource createModelResource(Tuple3<String, MetadataType, String> resPathPair, long ts,
                byte[] byteArray, MetadataStore.MemoryMetaData data) throws IOException {
            JsonNode je = JsonUtil.readValue(byteArray, JsonNode.class);
            ObjectNode modelMap = JsonUtil.valueToTree(je);
            String modelUuid = modelMap.get("uuid").asText();
            String proj = resPathPair._1();
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                String uniqueUuid = getUniqueUuid(modelUuidMap, proj, modelUuid);
                if (!modelUuid.equals(uniqueUuid)) {
                    modelUuid = uniqueUuid;
                    modelMap.put("uuid", modelUuid);
                    modelMap.put("meta_key", modelUuid);
                }
            }
            ArrayNode ccUuids = modelMap.putArray("computed_column_uuids");
            createAndSaveCcRelations(je, modelMap, proj, modelUuid, ts, data, ccUuids);
            createAndSaveTableRelations(je, modelMap, proj, modelUuid, ts, data);

            modelMap.put(PROJECT_KEY, proj);

            byte[] bytes = JsonUtil.writeValueAsIndentBytes(modelMap);
            RawResource res = JsonUtil.readValue(bytes, resPathPair._2().getResourceClass());
            res.setProject(proj);
            res.setContent(bytes);
            return res;
        }

        private RawResource createLayoutResource(Tuple3<String, MetadataType, String> resPathPair, byte[] byteArray)
                throws IOException {
            String proj = resPathPair._1();
            String dataflowUuid = resPathPair._3().split("/")[0];
            String layoutUuid = resPathPair._3().split("/")[1].replace(".json", "");
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                String uniqueModelUuid = getUniqueUuid(modelUuidMap, proj, dataflowUuid);
                String uniqueSegUuid = getUniqueUuid(segUuidMap, uniqueModelUuid, layoutUuid);
                if (!dataflowUuid.equals(uniqueModelUuid)) {
                    dataflowUuid = uniqueModelUuid;
                }
                if (!layoutUuid.equals(uniqueSegUuid)) {
                    layoutUuid = uniqueSegUuid;
                }
            }
            LayoutRawResource resource = JsonUtil.readValue(byteArray, LayoutRawResource.class);
            resource.setDataflowId(dataflowUuid);
            resource.setMetaKey(layoutUuid);
            resource.setUuid(resource.getMetaKey());
            ((RawResource) resource).setProject(proj);
            LinkedHashMap<Object, Object> layoutMap = JsonUtil.readValue(ByteSource.wrap(byteArray).openStream(),
                    new TypeReference<LinkedHashMap<Object, Object>>() {
                    });
            layoutMap.put("dataflow", resource.getDataflowId());
            layoutMap.put("uuid", resource.getUuid());
            layoutMap.put(PROJECT_KEY, proj);
            resource.setContent(JsonUtil.writeValueAsIndentBytes(layoutMap));
            return resource;
        }

        private RawResource createIndexPlanResource(Tuple3<String, MetadataType, String> resPathPair, byte[] byteArray)
                throws IOException {
            String proj = resPathPair._1();
            LinkedHashMap<Object, Object> otherMap = JsonUtil.readValue(ByteSource.wrap(byteArray).openStream(),
                    new TypeReference<LinkedHashMap<Object, Object>>() {
                    });
            String indexPlanUuid = otherMap.get("uuid").toString();
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                String uniqueUuid = getUniqueUuid(modelUuidMap, proj, indexPlanUuid);
                if (!indexPlanUuid.equals(uniqueUuid)) {
                    indexPlanUuid = uniqueUuid;
                }
            }
            otherMap.put(PROJECT_KEY, proj);
            otherMap.put("uuid", indexPlanUuid);
            RawResource res = JsonUtil.readValue(byteArray, resPathPair._2().getResourceClass());
            res.setProject(proj);
            res.setUuid(indexPlanUuid);
            res.setMetaKey(indexPlanUuid);
            res.setContent(JsonUtil.writeValueAsIndentBytes(otherMap));
            return res;
        }

        private RawResource createDefaultResource(Tuple3<String, MetadataType, String> resPathPair, byte[] byteArray)
                throws IOException {
            RawResource res = JsonUtil.readValue(byteArray, resPathPair._2().getResourceClass());
            if (resPathPair._2() != MetadataType.PROJECT) {
                String proj = resPathPair._1();
                LinkedHashMap<Object, Object> otherMap = JsonUtil.readValue(ByteSource.wrap(byteArray).openStream(),
                        new TypeReference<LinkedHashMap<Object, Object>>() {
                        });
                otherMap.put(PROJECT_KEY, proj);
                res.setProject(proj);
                res.setContent(JsonUtil.writeValueAsIndentBytes(otherMap));
            } else {
                res.setContent(byteArray);
            }
            return res;
        }

        public RawResource createDataParserResource(Tuple3<String, MetadataType, String> resPathPair, byte[] byteArray)
                throws IOException {
            String proj = resPathPair._1();
            LinkedHashMap<Object, Object> otherMap = JsonUtil.readValue(ByteSource.wrap(byteArray).openStream(),
                    new TypeReference<LinkedHashMap<Object, Object>>() {
                    });
            otherMap.put(PROJECT_KEY, proj);
            DataParserRawResource res = JsonUtil.readValue(byteArray, DataParserRawResource.class);
            res.setProject(proj);
            res.setMetaKey(proj + "." + res.getClassName());
            res.setContent(JsonUtil.writeValueAsIndentBytes(otherMap));
            return res;
        }
    }

    public static boolean verifyNonMetadataFile(String resourcePath) {
        if (resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        String[] nonMetadata = new String[] { "kylin.properties", ".DS_Store" };
        String[] metadataShouldBeIgnored = new String[] { "/rule/", "/query_history_id_offset/", "_global/epoch",
                "/execute/", "/recommendation/", "/accelerate_ratio/", "/favorite/", "/query_history_time_offset/",
                "/query_history/", "/query_history_time/", "/query_history_id/", "/event/", "/rec_items/",
                "/loading_range/", "/dataflow_detail/", "/async_task/" };
        String[] metadataWithoutJsonPostfix = new String[] { METASTORE_UUID_META_KEY_TAG, VERSION_FILE_META_KEY_TAG,
                METASTORE_IMAGE_META_KEY_TAG, "_global/user", "_global/user_group", "_global/sys_acl", "/streaming/" };

        for (String s : nonMetadata) {
            if (resourcePath.endsWith(s)) {
                return true;
            }
        }
        for (String s : metadataShouldBeIgnored) {
            if (s.startsWith("/") ? resourcePath.contains(s) : resourcePath.startsWith(s)) {
                return true;
            }
        }
        for (String s : metadataWithoutJsonPostfix) {
            if (s.startsWith("/") ? resourcePath.contains(s) : resourcePath.startsWith(s)) {
                return false;
            }
        }
        return !resourcePath.endsWith(".json");
    }

}
