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

package org.apache.kylin.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.engine.JobEngineConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Hold reusable steps for builders.
 */
public class JobBuilderSupport {

    final protected JobEngineConfig config;
    final protected CubeSegment seg;
    final protected String submitter;
    final protected Integer priorityOffset;

    final public static String LayeredCuboidFolderPrefix = "level_";

    final public static String PathNameCuboidBase = "base_cuboid";
    final public static String PathNameCuboidOld = "old";
    final public static String PathNameCuboidInMem = "in_memory";
    final public static Pattern JOB_NAME_PATTERN = Pattern.compile("kylin-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    public JobBuilderSupport(CubeSegment seg, String submitter) {
        this(seg, submitter, 0);
    }

    public JobBuilderSupport(CubeSegment seg, String submitter, Integer priorityOffset) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        this.config = new JobEngineConfig(seg.getConfig());
        this.seg = seg;
        this.submitter = submitter;
        this.priorityOffset = priorityOffset;
    }

    // ============================================================================

    public String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }

    public String getRealizationRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getRealization().getName();
    }

    public String getCuboidRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/cuboid/";
    }

    public String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }

    public void appendMapReduceParameters(StringBuilder buf) {
        appendMapReduceParameters(buf, JobEngineConfig.DEFAULT_JOB_CONF_SUFFIX);
    }

    public void appendMapReduceParameters(StringBuilder buf, String jobType) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(jobType);
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getOptimizationRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/optimize";
    }

    // ============================================================================
    // static methods also shared by other job flow participant
    // ----------------------------------------------------------------------------

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return getJobWorkingDir(conf.getHdfsWorkingDirectory(), jobId);
    }

    public static String getJobWorkingDir(String hdfsDir, String jobId) {
        if (!hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir + "/";
        }
        return hdfsDir + "kylin-" + jobId;
    }

    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

    public static String getCuboidOutputPathsByLevel(String cuboidRootPath, int level) {
        if (level == 0) {
            return cuboidRootPath + LayeredCuboidFolderPrefix + PathNameCuboidBase;
        } else {
            return cuboidRootPath + LayeredCuboidFolderPrefix + level + "_cuboid";
        }
    }

    public static String getBaseCuboidPath(String cuboidRootPath) {
        return cuboidRootPath + PathNameCuboidBase;
    }

    public static String getInMemCuboidPath(String cuboidRootPath) {
        return cuboidRootPath + PathNameCuboidInMem;
    }

    public String getDumpMetadataPath(String jobId) {
        return getRealizationRootPath(jobId) + "/metadata";
    }

    public static String extractJobIDFromPath(String path) {
        Matcher matcher = JOB_NAME_PATTERN.matcher(path);
        // check the first occurrence
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new IllegalStateException("Can not extract job ID from file path : " + path);
        }
    }

    public String getSegmentMetadataUrl(KylinConfig kylinConfig, String jobId) {
        Map<String, String> param = new HashMap<>();
        param.put("path", getDumpMetadataPath(jobId));
        return new StorageURL(kylinConfig.getMetadataUrl().getIdentifier(), "hdfs", param).toString();
    }

    public static void scanFiles(String input, FileSystem fs, List<FileStatus> outputs) throws IOException {
        Path path = new Path(input);
        if (!fs.exists(path)) {
            return;
        }
        FileStatus[] fileStatuses = fs.listStatus(path, p -> !p.getName().startsWith("_"));
        for (FileStatus stat : fileStatuses) {
            if (stat.isDirectory()) {
                scanFiles(stat.getPath().toString(), fs, outputs);
            } else {
                outputs.add(stat);
            }
        }
    }

    public static long getFileSize(String input, FileSystem fs) throws IOException {
        List<FileStatus> outputs = Lists.newArrayList();
        scanFiles(input, fs, outputs);
        long size = 0L;
        for (FileStatus stat: outputs) {
            size += stat.getLen();
        }
        return size;
    }
}
