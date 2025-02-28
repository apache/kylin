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

package org.apache.kylin.tool.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;

import lombok.SneakyThrows;

public class JobMetadataWriter {

    public static void writeJobMetaData(KylinConfig config, RawResource jobMeta, String project) {
        JobContextUtil.cleanUp();
        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(config);

        long updateTime = jobMeta.getTs();
        ExecutablePO executablePO = parseExecutablePO(jobMeta.getByteSource(), updateTime, project);
        jobInfoDao.addJob(executablePO);
    }

    @SneakyThrows
    public static void writeJobMetaData(KylinConfig config) {
        String jobId = "dd5a6451-0743-4b32-b84d-2ddc8052429f";
        File jobMeta = Paths.get("src/test/resources/ut_job_exec", jobId).toFile();
        RawResource jobRaw = new RawResource();
        try (FileInputStream fis = new FileInputStream(jobMeta)) {
            byte[] content = IOUtils.toByteArray(fis);
            jobRaw.setContent(content);
            jobRaw.setTs(jobMeta.lastModified());
            jobRaw.setMvcc(0);
        }
        JobMetadataWriter.writeJobMetaData(config, jobRaw, "newten");
    }

    private static ExecutablePO parseExecutablePO(ByteSource byteSource, long updateTime, String projectName) {
        try {
            return JobInfoUtil.deserializeExecutablePO(byteSource, updateTime, projectName);
        } catch (IOException e) {
            throw new RuntimeException("Error when deserializing job metadata", e);
        }
    }
}
