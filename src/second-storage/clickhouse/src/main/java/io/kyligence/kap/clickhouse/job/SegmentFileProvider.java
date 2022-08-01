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

package io.kyligence.kap.clickhouse.job;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SegmentFileProvider implements FileProvider {
    private final String rootPath;

    public SegmentFileProvider(String rootPath) {
        this.rootPath = rootPath;
    }

    @Override
    public List<FileStatus> getAllFilePaths() {
        List<FileStatus> paths = new ArrayList<>();
        final FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(rootPath), true);
            while (it.hasNext()) {
                LocatedFileStatus fileStatus = it.next();
                if (!fileStatus.getPath().getName().endsWith("parquet")) {
                    continue;
                }
                paths.add(FileStatus.builder()
                        .path(fileStatus.getPath().toString())
                        .len(fileStatus.getLen())
                        .build());
            }
        } catch (IOException e) {
            return ExceptionUtils.rethrow(new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e));
        }
        return paths;
    }

}
