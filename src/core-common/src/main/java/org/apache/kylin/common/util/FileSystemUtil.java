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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.constant.ObsConfig;

public class FileSystemUtil {
    public static final String S3_FILE_SYSTEM_CLASS = "S3AFileSystem";
    public static final String OSS_FILE_SYSTEM_CLASS = "AliyunOSSFileSystem";

    public static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
        FileStatus[] statuses = fs.listStatus(path);
        if (fs.getClass().getName().endsWith(S3_FILE_SYSTEM_CLASS)) {
            // The fileStatus of directory returned by fs.listStatus cannot get last modificationTime.
            // Let's get it via fs.getFileStatus.
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i].isDirectory()) {
                    statuses[i] = fs.getFileStatus(statuses[i].getPath());
                }
            }
        }
        return statuses;
    }

    public static Map<String, String> generateRoleCredentialConf(String type, String bucket, String role,
            String endpoint, String region) {
        Map<String, String> conf = new HashMap<>();
        ObsConfig obsConfig = ObsConfig.getByType(type).orElse(ObsConfig.S3);
        if (StringUtils.isNotEmpty(role)) {
            conf.put(String.format(Locale.ROOT, obsConfig.getRoleArnKey(), bucket), role);
            conf.put(String.format(Locale.ROOT, obsConfig.getCredentialProviderKey(), bucket),
                    obsConfig.getCredentialProviderValue());
            conf.put(String.format(Locale.ROOT, obsConfig.getAssumedRoleCredentialProviderKey(), bucket),
                    obsConfig.getAssumedRoleCredentialProviderValue());
        }
        if (StringUtils.isNotEmpty(endpoint)) {
            conf.put(String.format(Locale.ROOT, obsConfig.getEndpointKey(), bucket), endpoint);
        }
        if (StringUtils.isNotEmpty(region)) {
            conf.put(String.format(Locale.ROOT, obsConfig.getRegionKey(), bucket), region);
        }
        return conf;
    }
}
