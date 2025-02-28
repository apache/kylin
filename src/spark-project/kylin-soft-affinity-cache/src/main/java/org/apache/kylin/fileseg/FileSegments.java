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

package org.apache.kylin.fileseg;

import static org.apache.kylin.metadata.model.TableExtDesc.LOCATION_PROPERTY_KEY;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Sets;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FilePartitionDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.PartitionDesc.IPartitionConditionBuilder;
import org.apache.kylin.metadata.model.PartitionDesc.PartitionType;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TimePartitionedSegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A segment partition that is based on source files.
 * <p/>
 * Each segment is built from one source file.
 * <p/>
 * Term:
 * - FILE HASH: a (hash) code that represents a file, typically made up of filename, last modified, size etc.
 */
@Slf4j
@SuppressWarnings({ "rawtypes" })
public class FileSegments {
    // used in SparkContext local property
    public static String SOURCE_FILTER_KEY = "spark.kylin.fileseg.filter";

    public static String FILE_HASH_SEP = ":";

    /**
     * Set file segment filter on spark context locally, happens at the DRIVER SIDE.
     */
    public static void setFileSegFilterLocally(NDataSegment seg, FileSegRange segRange) {
        Preconditions.checkState(SparkSession.getDefaultSession().isDefined());
        SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String sourceFileDir = getSegSourceFileDir(seg, config);
        String sourceFileHash = String.join(FILE_HASH_SEP, segRange.getFileHash());

        // The sourceFileDir metadata is at "table_exd/data_source_properties/location", 
        // value like "s3://maosha-public/liyang/cloud_billings"
        // The sourceFileHash is a relative path based on the dir, like "cloud_billing_standard_sample.csv"

        sparkContext.setLocalProperty(pathFilterKey(sourceFileDir), sourceFileHash);
    }

    private static String noEndingSlash(String p) {
        return p.endsWith("/") ? p.substring(0, p.length() - 1) : p;
    }

    private static String pathFilterKey(String path) {
        return SOURCE_FILTER_KEY + "." + noEndingSlash(path);
    }

    /**
     * Get file segment filter on spark context locally, happens at the EXECUTOR SIDE.
     */
    public static Optional<SourceFilePredicate> getFileSegFilterLocally(String srcDirPath) {
        String srcFileHash = getSparkLocalProperty(pathFilterKey(srcDirPath), null);

        if (srcFileHash == null)
            return Optional.empty();
        else
            return Optional.of(new SourceFilePredicate(srcDirPath, Sets.newHashSet(srcFileHash.split(FILE_HASH_SEP))));
    }

    private static String getSparkLocalProperty(String key, @SuppressWarnings("SameParameterValue") String dft) {
        if (TaskContext.get() != null) {
            String prop = TaskContext.get().getLocalProperty(key);
            if (prop != null)
                return prop;
        } else if (SparkSession.getDefaultSession().isDefined()) {
            SparkContext ctx = SparkSession.getDefaultSession().get().sparkContext();
            String prop = ctx.getLocalProperty(key);
            if (prop != null)
                return prop;
        }
        return dft;
    }

    public static void clearFileSegFilterLocally() {
        if (SparkSession.getDefaultSession().isDefined()) {
            SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
            List<String> keys = sparkContext.getLocalProperties().keySet().stream().map(Object::toString)
                    .collect(Collectors.toList());
            for (String key : keys) {
                if (key.startsWith(SOURCE_FILTER_KEY))
                    sparkContext.setLocalProperty(key, null);
            }
        }
    }

    private static String getSegSourceFileDir(NDataSegment seg, KylinConfig config) {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, seg.getProject());
        TableDesc rootFactTable = seg.getDataflow().getModel().getRootFactTable().getTableDesc();
        TableExtDesc tableExt = tableManager.getTableExtIfExists(rootFactTable);
        Preconditions.checkNotNull(tableExt);
        String ret = tableExt.getDataSourceProps().get(LOCATION_PROPERTY_KEY);
        Preconditions.checkNotNull(ret);
        return ret;
    }

    public static String computeFileHash(FileStatus file) {
        return file.getPath().getName() + "/" + Long.toString(file.getLen(), 36) + "/"
                + Long.toString(file.getModificationTime(), 36);
    }

    public static String hashToName(String fileHash) {
        int cut = fileHash.indexOf('/');
        if (cut < 0)
            throw new IllegalArgumentException("Invalid file hash: " + fileHash);
        return fileHash.substring(0, cut);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isFileSegment(NDataSegment seg) {
        return seg.getSegRange() instanceof FileSegRange;
    }

    public static void forceFileSegments(String project, String modelId, String storageLocation,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<List<String>> fileHashOpt,
            Function<FileSegRange, NDataSegment> segCreator, Function<String, Boolean> segDeleter) {
        KylinConfig config = NProjectManager.getProjectConfig(project);

        // ensure model has a file based partition
        NDataModelManager modelManager = config.getManager(project, NDataModelManager.class);
        NDataModel model = modelManager.getDataModelDesc(modelId);
        if (model.isBroken())
            return;

        if (!model.isFilePartitioned()) {
            FilePartitionDesc newPart = new FilePartitionDesc();
            newPart.setCubePartitionType(PartitionType.FILE);
            newPart.setFileStorageLocation(noEndingSlash(storageLocation));
            newPart.setPartitionConditionBuilderClz(FileSegmentConditionBuilder.class.getName());
            newPart.setPartitionDateColumn(""); // no use, just for code compatibility
            newPart.setPartitionDateFormat("yyyy-MM-dd"); // no use, just for code compatibility
            modelManager.updateDataModel(modelId, (modelCopy) -> modelCopy.setPartitionDesc(newPart));
        }

        List<String> fileHashs = filterFileHashs(fileHashOpt, project + "/" + model.getAlias(), config);
        if (CollectionUtils.isEmpty(fileHashs)) {
            return; // just ensure the model is file-segment enabled, if fileHashs is missing
        }

        NDataflowManager dfManager = config.getManager(project, NDataflowManager.class);
        NDataflow df = dfManager.getDataflow(modelId); // yes, dataflow_id == model_id
        if (df.isBroken())
            return;

        // mark this data refresh time
        dfManager.updateDataflow(df.getId(), (copy) -> copy.setLastDataRefreshTime(System.currentTimeMillis()));

        // delete all non-file segments
        // delete all file-not-exist segments
        // add all new-file segments
        List<NDataSegment> origSegments = ImmutableList.copyOf(df.getSegments());
        Set<String> latestHashs = new HashSet<>(fileHashs);
        Set<String> toCreateHashs = new LinkedHashSet<>(fileHashs);
        List<String> toDelSegIds = new ArrayList<>();
        for (NDataSegment orig : origSegments) {
            if (!isFileSegment(orig)) {
                toDelSegIds.add(orig.getId());
                continue;
            }
            List<String> origHash = ((FileSegRange) orig.getSegRange()).getFileHash();
            if (latestHashs.containsAll(origHash)) {
                //noinspection SlowAbstractSetRemoveAll
                toCreateHashs.removeAll(origHash);
            } else {
                toDelSegIds.add(orig.getId());
            }
        }

        // do the deletion
        for (String toDelSegId : toDelSegIds) {
            try {
                segDeleter.apply(toDelSegId);
            } catch (Exception ex) {
                log.error("delete segment failed {}/{}", df.getId(), toDelSegId, ex);
            }
        }

        // do the creation
        FileRangeGenerator gen = new FileRangeGenerator(
                origSegments.stream().filter(seg -> !toDelSegIds.contains(seg.getId())).collect(Collectors.toList()));
        for (String toCreateHash : toCreateHashs) {
            try {
                segCreator.apply(gen.nextFileSegRange(toCreateHash));
            } catch (Exception ex) {
                log.error("create segment failed {}/{}", df.getId(), toCreateHash, ex);
            }
        }

        // logging
        ModelFileSegments result = getModelFileSegments(project, modelId);
        log.info("Sync file segments for {}/{}, {} created, {} deleted, results {}/{}/{}/{}", project, result.model,
                toCreateHashs.size(), toDelSegIds.size(), result.summary.get(FileSegStatusEnum.LOADED),
                result.summary.get(FileSegStatusEnum.LOADING), result.summary.get(FileSegStatusEnum.EXIST),
                result.summary.get(FileSegStatusEnum.WARNING));
    }

    private static List<String> filterFileHashs(Optional<List<String>> fileHashOpt, String modelForLog,
            KylinConfig config) {
        if (!fileHashOpt.isPresent())
            return Collections.emptyList();

        List<String> hashList = fileHashOpt.get();
        String successFile = config.getFileSegmentSuccessFile();
        boolean hasSuccessFile = false;

        List<String> ret = new ArrayList<>(hashList.size());
        for (String hash : hashList) {
            String fname = hashToName(hash);
            if (fname.equals(successFile)) {
                hasSuccessFile = true; // skip & mark success file
                continue;
            }
            if (fname.startsWith(".") || fname.startsWith("_")) {
                continue; // file starts with '.' or '_' are hidden files according to hadoop convention
            }

            ret.add(hash);
        }

        // a folder without success file is ignored
        if (successFile != null && !hasSuccessFile) {
            log.debug("Model {} ignored forceFileSegments() due to missing success file {} from {} detected files",
                    modelForLog, successFile, hashList.size());
            return Collections.emptyList();
        }

        return ret;
    }

    public static ModelFileSegments getModelFileSegments(String project, String modelId) {
        return getModelFileSegments(project, modelId, false);
    }

    public static ModelFileSegments getModelFileSegments(String project, String modelId, boolean returnDetails) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfManager = config.getManager(project, NDataflowManager.class);
        NDataflow df = dfManager.getDataflow(modelId); // yes, dataflow id == model id
        Preconditions.checkState(df.getModel().isFilePartitioned());

        // collect file->[segment status], note a file may have two segments, 
        // one is READY, another is NEW (building)
        Map<String, Set<NDataSegment>> fileToSegs = new LinkedHashMap<>();
        for (NDataSegment seg : df.getSegments()) {
            SegmentRange range = seg.getSegRange();
            if (!(range instanceof FileSegRange))
                continue; // quote impossible, though

            FileSegRange fileRange = (FileSegRange) range;
            for (String fileHash : fileRange.fileHash) {
                fileToSegs.compute(fileHash, (hash, set) -> {
                    if (set == null)
                        set = new HashSet<>();
                    set.add(seg);
                    return set;
                });
            }
        }

        List<FileSegStatus> segStatuses = Lists.newArrayList();
        int loading = 0;
        int loaded = 0;
        int warning = 0;
        int exist = 0;
        for (Entry<String, Set<NDataSegment>> entry : fileToSegs.entrySet()) {
            String fileHash = entry.getKey();
            Set<NDataSegment> segs = entry.getValue();
            FileSegStatusEnum status;
            if (containsSegLike(segs, SegmentStatusEnum.NEW)) {
                status = FileSegStatusEnum.LOADING;
                loading++;
            } else if (containsSegWithIndex(segs)) {
                status = FileSegStatusEnum.LOADED;
                loaded++;
            } else if (containsSegLike(segs, SegmentStatusEnum.WARNING)) {
                status = FileSegStatusEnum.WARNING;
                warning++;
            } else {
                status = FileSegStatusEnum.EXIST;
                exist++;
            }

            if (returnDetails) {
                segStatuses.add(new FileSegStatus(fileHash, status, 0));
            }
        }

        Map<FileSegStatusEnum, Integer> summary = ImmutableMap.of(FileSegStatusEnum.LOADED, loaded,
                FileSegStatusEnum.LOADING, loading, FileSegStatusEnum.EXIST, exist, FileSegStatusEnum.WARNING, warning);

        // check if any index is being built (not new segment, but new index)
        boolean isBuildingIndex = guessIsBuildingIndex(df);

        return new ModelFileSegments(project, df.getModelAlias(), df.getLastDataRefreshTime(),
                df.getIndexPlan().getLastModified(), loading > 0, isBuildingIndex, false, summary, segStatuses);
    }

    public static boolean guessIsBuildingIndex(NDataflow df) {
        Set<Long> plannedLayouts = df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        for (NDataSegment seg : df.getSegments()) {
            if (seg.getStatus() == SegmentStatusEnum.NEW)
                continue; // ignore new data loading
            if ("FULL_BUILD".equals(seg.getName()))
                break; // for file-seg-model, a FULL_BUILD seg means empty file folder, ignore
            Map<Long, NDataLayout> layouts = seg.getLayoutsMap();
            if (layouts == null || !layouts.keySet().containsAll(plannedLayouts)) {
                // some segment is missing planned layouts
                return true;
            }
        }
        return false;
    }

    private static boolean containsSegWithIndex(Set<NDataSegment> segs) {
        return segs.stream().anyMatch(seg -> seg.getLayoutSize() > 0);
    }

    private static boolean containsSegLike(Set<NDataSegment> segs, SegmentStatusEnum status) {
        return segs.stream().anyMatch(seg -> seg.getStatus() == status);
    }

    public static String makeSyncFileSegSql(String selectSql) {
        return "select count(*) _SYNC_FILE_SEGMENTS_ from ( " + selectSql + " )";
    }

    public static String makeSyncFileSegSql(List<NDataModel> models) {
        List<String> factTables = models.stream().map(NDataModel::getRootFactTableName).distinct()
                .collect(Collectors.toList());

        return factTables.stream().map(tbl -> "select count(*) _SYNC_FILE_SEGMENTS_ from " + tbl)
                .collect(Collectors.joining(" union all "));
    }

    public static boolean isSyncFileSegSql(String sql) {
        return sql.contains("_SYNC_FILE_SEGMENTS_");
    }

    public static List<NDataModel> findModelsOfFileSeg(String project, List<String> modelAliasOrFactTables) {
        if (modelAliasOrFactTables == null || modelAliasOrFactTables.isEmpty())
            return ImmutableList.of();

        List<NDataModel> models = listModelsOfFileSeg(project);
        return models.stream()
                .filter(m -> modelAliasOrFactTables.stream().anyMatch(
                        inp -> m.getAlias().equalsIgnoreCase(inp) || m.getRootFactTableName().equalsIgnoreCase(inp)))
                .collect(Collectors.toList());
    }

    public static List<NDataModel> listModelsOfFileSeg(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfManager = config.getManager(project, NDataflowManager.class);

        List<NDataModel> models = dfManager.listUnderliningDataModels();
        return models.stream()
                .filter(m -> m.isFilePartitioned()
                        && ((FilePartitionDesc) m.getPartitionDesc()).getFileStorageLocation() != null)
                .collect(Collectors.toList());
    }

    public enum FileSegStatusEnum {
        LOADED, LOADING, EXIST, WARNING
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ModelFileSegments {

        public static ModelFileSegments broken(String project, String modelAlias) {
            ModelFileSegments ret = new ModelFileSegments();
            ret.setProject(project);
            ret.setModel(modelAlias);
            ret.setBroken(true);
            return ret;
        }

        @JsonProperty("project")
        private String project;
        @JsonProperty("model")
        private String model;
        @JsonProperty("last_data_refresh_time")
        private long lastDataRefreshTime;
        @JsonProperty("last_index_refresh_time")
        private long lastIndexRefreshTime;
        @JsonProperty("is_loading_data")
        private boolean isLoadingData;
        @JsonProperty("is_building_index")
        private boolean isBuildingIndex;
        @JsonProperty("is_broken")
        private boolean isBroken;
        @JsonProperty("summary")
        private Map<FileSegStatusEnum, Integer> summary;
        @JsonProperty("segments")
        private List<FileSegStatus> segments;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FileSegStatus {
        @JsonProperty("file_hash")
        private String fileHash;
        @JsonProperty("status")
        private FileSegStatusEnum status;
        @JsonProperty("progress_bar")
        private Integer progressBar; // progress bar of 0-100
    }

    private static class FileRangeGenerator {
        final SimpleDateFormat dateFormat;
        int nextStartYear = 2000;

        FileRangeGenerator(List<NDataSegment> origSegments) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
            dateFormat.setTimeZone(TimeZone.getDefault());

            Optional<Long> maxEnd = origSegments.stream().map(seg -> (Long) seg.getSegRange().getEnd())
                    .reduce(Math::max);
            maxEnd.ifPresent(millis -> nextStartYear = yearOf(millis));

            // prevention of time range overflow
            if (nextStartYear > 22000) {
                nextStartYear = 1970;
                log.error("File segments time range running out?? SegIds: "
                        + origSegments.stream().map(seg -> seg.getId()).collect(Collectors.joining(",")));
            }
        }

        private int yearOf(long millis) {
            String format = dateFormat.format(new Date(millis));
            int cut = format.indexOf('-');
            return Integer.parseInt(format.substring(0, cut));
        }

        private long millisOf(int year) {
            try {
                return dateFormat.parse(year + "-01-01").getTime();
            } catch (ParseException e) {
                throw new KylinRuntimeException(e); // unreachable
            }
        }

        public FileSegRange nextFileSegRange(String toCreateHash) {
            FileSegRange ret = new FileSegRange();
            ret.setStart(millisOf(nextStartYear++));
            ret.setEnd(millisOf(nextStartYear));
            ret.setFileHash(ImmutableList.of(toCreateHash));
            return ret;
        }
    }

    @RequiredArgsConstructor
    public static class SourceFilePredicate {

        final String srcDir;
        final Set<String> srcFileHash;

        public boolean checkFile(LocatedFileStatus f) {
            if (f.isDirectory())
                return true;

            String thisFile = computeFileHash(f);
            boolean ok = srcFileHash.contains(thisFile);
            if (!ok) {
                log.trace("filtered out file {}, the filter is {}", thisFile, srcFileHash);
            }
            return ok;
        }
    }

    // used by reflection of full class name
    public static class FileSegmentConditionBuilder implements IPartitionConditionBuilder, Serializable {
        @Override
        public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {

            setFileSegFilterLocally((NDataSegment) seg, (FileSegRange) segRange);

            return "1=1";
        }

        @Override
        public String buildMultiPartitionCondition(PartitionDesc partDesc, MultiPartitionDesc multiPartDesc,
                LinkedList<Long> partitionIds, ISegment seg, SegmentRange segRange) {
            return "1=1";
        }
    }

    /**
     * This SHALL BETTER implement SegmentRange DIRECTLY.
     * But TimePartitionedSegmentRange is hard-coded at too many places, extending it is a temp workaround.
     */
    @Getter
    @Setter
    public static class FileSegRange extends TimePartitionedSegmentRange {
        @JsonProperty("file_hash")
        private List<String> fileHash;

        @Override
        protected void checkSameType(SegmentRange<Long> o) {
            Preconditions.checkNotNull(o);
        }

        @Override
        public SegmentRange<Long> coverWith(SegmentRange<Long> other) {
            FileSegRange o = (FileSegRange) other;
            FileSegRange ret = new FileSegRange();
            ret.start = Math.min(this.start, o.start);
            ret.end = Math.max(this.end, o.end);
            ret.fileHash = new ArrayList<>();
            ret.fileHash.addAll(Objects.firstNonNull(this.fileHash, Collections.emptyList()));
            ret.fileHash.addAll(Objects.firstNonNull(o.fileHash, Collections.emptyList()));
            return ret;
        }

        @Override
        public String proposeSegmentName() {
            if (fileHash == null || fileHash.isEmpty())
                return null;
            else
                return String.join("|", fileHash);
        }
    }

}
