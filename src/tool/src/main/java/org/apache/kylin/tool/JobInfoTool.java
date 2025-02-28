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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfoTool extends CancelableTask {
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String JOB_INFO_DIR = "job_info";
    private static final String ZIP_SUFFIX = ".zip";

    public void backup(String dir, String project) throws IOException {
        extractToHDFS(dir + "/" + JOB_INFO_DIR, project);
    }

    public void restore(String dir, boolean afterTruncate) throws IOException {
        Path path = new Path(dir + "/" + JOB_INFO_DIR);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            String fileName = fileStatus.getPath().getName();
            String project = fileName.substring(0, fileName.indexOf("."));
            restoreProject(dir, project, afterTruncate);
        }
    }

    public void restoreProject(String dir, String project, boolean afterTruncate) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path path = new Path(dir + "/" + JOB_INFO_DIR + "/" + project + ZIP_SUFFIX);
        List<JobInfo> jobInfos = Lists.newArrayList();
        try (ZipInputStream zis = new ZipInputStream(fs.open(path));
                BufferedReader br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
            while (zis.getNextEntry() != null) {
                String value = br.readLine();
                JobInfoHelper jobInfo = JsonUtil.readValue(value, JobInfoHelper.class);
                jobInfo.setJobContent();
                jobInfos.add(jobInfo);
            }
        }
        JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv()).restoreJobInfo(jobInfos, project, afterTruncate);
    }

    public void extractFull(File dir) {
        JobMapperFilter filter = JobMapperFilter.builder().build();
        List<JobInfo> jobs = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                .getJobInfoListByFilter(filter);
        for (JobInfo job : jobs) {
            saveJobToFile(job, dir);
        }
    }

    public void extractFull(File dir, long startTime, long endTime) {
        JobMapperFilter filter = JobMapperFilter.builder().timeRange(Arrays.asList(startTime, endTime)).build();
        List<JobInfo> jobs = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                .getJobInfoListByFilter(filter);
        for (JobInfo job : jobs) {
            saveJobToFile(job, dir);
        }
    }

    public void extractJob(File dir, String project, String jobId) {
        JobMapperFilter filter = JobMapperFilter.builder().project(project).jobId(jobId).build();
        List<JobInfo> jobs = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                .getJobInfoListByFilter(filter);
        if (!jobs.isEmpty()) {
            saveJobToFile(jobs.get(0), dir);
        } else {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Job id {%s} not found.", jobId));
        }
    }

    private void saveJobToFile(JobInfo job, File dir) {
        File jobFile = new File(dir, job.getJobId());
        try (OutputStream os = Files.newOutputStream(jobFile.toPath());
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            bw.write(JsonUtil.writeValueAsString(new JobInfoHelper(job)));
            bw.newLine();
        } catch (Exception e) {
            logger.error("Write error, id is {}", job.getId(), e);
        }
    }

    public void extractJobLock(File dir) throws Exception {
        List<JobLock> jobLocks = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv()).fetchAllJobLock();
        File jobLockFile = new File(dir, "job_lock");
        try (OutputStream os = new FileOutputStream(jobLockFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (JobLock line : jobLocks) {
                try {
                    bw.write(JsonUtil.writeValueAsString(line));
                    bw.newLine();
                } catch (Exception e) {
                    logger.error("Write error, id is {}", line.getId(), e);
                }
            }
        }
    }

    public void extractToHDFS(String dir, String project) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String filePathStr = dir + "/" + project + ZIP_SUFFIX;
        try (FSDataOutputStream fos = fs.create(new Path(filePathStr));
                ZipOutputStream zos = new ZipOutputStream(fos);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zos, Charset.defaultCharset()))) {
            JobMapperFilter filter = JobMapperFilter.builder().project(project).build();
            List<JobInfo> jobs = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                    .getJobInfoListByFilter(filter);
            for (JobInfo job : jobs) {
                zos.putNextEntry(new ZipEntry(job.getJobId() + ".json"));
                String value = JsonUtil.writeValueAsString(new JobInfoHelper(job));
                bw.write(value);
                bw.flush();
                if (isCanceled()) {
                    logger.info("job info backup was canceled.");
                    return;
                }
            }
        }
    }

    public void backupToLocal(String dir, String project) throws IOException {
        String extractDir = prepareLocalBackupDir(dir, project);
        JobMapperFilter filter = JobMapperFilter.builder().project(project).build();
        List<JobInfo> jobs = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                .getJobInfoListByFilter(filter);

        for (JobInfo job : jobs) {
            String fileName = extractDir + job.getJobId() + ".json";
            try (BufferedWriter bf = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(new File(fileName).toPath()), StandardCharsets.UTF_8))) {
                String value = JsonUtil.writeValueAsString(new JobInfoHelper(job));
                bf.write(value);
                bf.flush();
            }
        }
    }

    private String prepareLocalBackupDir(String dir, String project) throws IOException {
        File rootDir = new File(dir);
        if (!rootDir.exists()) {
            FileUtils.forceMkdir(rootDir);
        }
        String jobInfoDirPath = StringUtils.appendIfMissing(dir, "/") + JOB_INFO_DIR;
        File jobInfoDir = new File(jobInfoDirPath);
        if (!jobInfoDir.exists()) {
            FileUtils.forceMkdir(jobInfoDir);
        }
        String extractDirPath = jobInfoDirPath + "/" + project + "/";
        File extractDir = new File(extractDirPath);
        if (!extractDir.exists()) {
            FileUtils.forceMkdir(extractDir);
        }
        return extractDirPath;
    }

    public void restoreFromLocal(String dir, boolean afterTruncate) throws IOException {
        String restorePath = dir + "/" + JOB_INFO_DIR;
        File restoreDir = new File(restorePath);
        if (!restoreDir.exists()) {
            return;
        }
        File[] projectDirs = restoreDir.listFiles();
        for (File projectDir : projectDirs) {
            String project = projectDir.getName();
            restoreProjectFromLocal(dir, project, afterTruncate);
        }
    }

    public void restoreProjectFromLocal(String dir, String project, boolean afterTruncate) throws IOException {
        String restoreProjectPath = dir + "/" + JOB_INFO_DIR + "/" + project;
        File restoreProjectDir = new File(restoreProjectPath);
        if (!restoreProjectDir.exists()) {
            return;
        }
        File[] jsonFiles = restoreProjectDir.listFiles();
        List<JobInfo> jobInfos = Lists.newArrayList();
        assert jsonFiles != null;
        for (File jsonFile : jsonFiles) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(jsonFile.toPath()), StandardCharsets.UTF_8))) {
                String value = br.readLine();
                JobInfoHelper jobInfo = JsonUtil.readValue(value, JobInfoHelper.class);
                jobInfo.setJobContent();
                jobInfos.add(jobInfo);
            }
        }
        JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv()).restoreJobInfo(jobInfos, project, afterTruncate);
    }

    static class JobInfoHelper extends JobInfo {
        private ExecutablePO jobContentJson;

        public JobInfoHelper() {
        }

        public JobInfoHelper(JobInfo jobInfo) {
            this.setId(jobInfo.getId());
            this.setCreateTime(jobInfo.getCreateTime());
            this.setJobDurationMillis(jobInfo.getJobDurationMillis());
            this.setJobId(jobInfo.getJobId());
            this.setJobStatus(jobInfo.getJobStatus());
            this.setJobType(jobInfo.getJobType());
            this.setModelId(jobInfo.getModelId());
            this.setMvcc(jobInfo.getMvcc());
            this.setProject(jobInfo.getProject());
            this.setSubject(jobInfo.getSubject());
            this.setUpdateTime(jobInfo.getUpdateTime());
            this.setPriority(jobInfo.getPriority());

            jobContentJson = JobInfoUtil.deserializeExecutablePO(jobInfo);
        }

        public ExecutablePO getJobContentJson() {
            return jobContentJson;
        }

        public void setJobContent() {
            this.setJobContent(JobInfoUtil.serializeExecutablePO(jobContentJson));
        }
    }
}
