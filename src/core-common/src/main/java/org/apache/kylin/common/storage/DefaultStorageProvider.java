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
package org.apache.kylin.common.storage;

import java.io.IOException;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import lombok.Data;

// for reflection
public class DefaultStorageProvider implements IStorageProvider {
    /**
     * Warning: different cloud provider may not return full ContentSummary,
     * only return file length and count now.
     * @param fileSystem
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public ContentSummary getContentSummary(FileSystem fileSystem, Path path) throws IOException {
        try {
            return fileSystem.getContentSummary(path);
        } catch (AccessControlException ae) {
            ContentSummaryBean bean = recursive(fileSystem, path);
            ContentSummary.Builder builder = new ContentSummary.Builder();
            return builder.fileCount(bean.getFileCount()).length(bean.getLength()).build();
        }
    }

    @Data
    class ContentSummaryBean {
        long fileCount = 0;
        long length = 0;
    }

    public ContentSummaryBean recursive(FileSystem fs, Path path) throws IOException {
        ContentSummaryBean result = new ContentSummaryBean();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            if (fileStatus.isDirectory()) {
                ContentSummaryBean bean = recursive(fs, fileStatus.getPath());
                result.setFileCount(result.getFileCount() + bean.getFileCount());
                result.setLength(result.getLength() + bean.getLength());
            } else {
                result.setFileCount(result.getFileCount() + 1);
                result.setLength(result.getLength() + fileStatus.getLen());
            }
        }

        return result;
    }
}
