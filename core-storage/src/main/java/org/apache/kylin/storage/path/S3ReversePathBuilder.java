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

package org.apache.kylin.storage.path;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cube.CubeSegment;

public class S3ReversePathBuilder implements IStoragePathBuilder {

    protected String prefix;
    protected String bucketName;

    protected String getReverseMetaDir(String workingDir) {
        int prefixIndex = workingDir.indexOf("//") + 2;
        int dirIndex = workingDir.indexOf(SLASH, prefixIndex);

        this.prefix = workingDir.substring(0, prefixIndex);
        this.bucketName = workingDir.substring(prefixIndex, dirIndex);

        String[] dirs = workingDir.substring(dirIndex + 1).split(SLASH);
        ArrayUtils.reverse(dirs);

        return StringUtils.join(dirs, SLASH);
    }

    @Override
    public String getJobWorkingDir(String workingDir, String jobId) {
        String reverseMetaDir = getReverseMetaDir(workingDir);

        return this.prefix + this.bucketName + SLASH + "kylin-" + jobId + SLASH + reverseMetaDir;
    }

    @Override
    public String getJobRealizationRootPath(CubeSegment cubeSegment, String jobId) {
        String jobWorkingDir = getJobWorkingDir(cubeSegment.getConfig().getHdfsWorkingDirectory(), jobId);

        return jobWorkingDir + SLASH + cubeSegment.getRealization().getName();
    }

    @Override
    public String getRealizationFinalDataPath(CubeSegment cubeSegment) {
        String reverseMetaDir = getReverseMetaDir(cubeSegment.getConfig().getHdfsWorkingDirectory());
        String cubeName = cubeSegment.getRealization().getName();
        String segName = cubeSegment.getName();

        return this.prefix + this.bucketName + SLASH + segName + SLASH + cubeName + SLASH + reverseMetaDir;
    }
}
