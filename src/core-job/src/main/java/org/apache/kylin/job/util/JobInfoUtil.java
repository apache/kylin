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
package org.apache.kylin.job.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobInfoUtil {

    public static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);

    public static byte[] serializeExecutablePO(ExecutablePO executablePO) {
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
             DataOutputStream dout = new DataOutputStream(buf);) {
            JOB_SERIALIZER.serialize(executablePO, dout);
            ByteSource byteSource = ByteSource.wrap(buf.toByteArray());
            return byteSource.read();
        } catch (IOException e) {
            throw new RuntimeException("Serialize ExecutablePO failed, id: " + executablePO.getId(), e);
        }
    }

    public static ExecutablePO deserializeExecutablePO(JobInfo jobInfo) {
        try {
            ByteSource byteSource = ByteSource.wrap(CompressionUtils.decompress(jobInfo.getJobContent()));
            return deserializeExecutablePO(byteSource, jobInfo.getUpdateTime(), jobInfo.getProject());
        } catch (IOException | DataFormatException e) {
            log.warn("Error when deserializing jobInfo, id: {} " + jobInfo.getJobId(), e);
            return null;
        }
    }

    public static ExecutablePO deserializeExecutablePO(ByteSource byteSource, long updateTime, String project)
            throws IOException {
        try (InputStream is = byteSource.openStream(); DataInputStream din = new DataInputStream(is)) {
            ExecutablePO r = JOB_SERIALIZER.deserialize(din);
            r.setLastModified(updateTime);
            r.setProject(project);
            return r;
        }
    }
}
