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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.kylin.stream.core.storage.columnar.FragmentId;
import org.apache.kylin.stream.core.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public abstract class ColumnarSplitInputFormat extends FileInputFormat {
    private static final Logger logger = LoggerFactory.getLogger(ColumnarSplitInputFormat.class);

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

        List<FileStatus> result = Lists.newArrayList();
        for (int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            final FileSystem fs = p.getFileSystem(job.getConfiguration());
            FileStatus[] groups = fs.listStatus(p, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    String groupName = path.getName();
                    String groupNameRegex = "\\d+";
                    return groupName.matches(groupNameRegex);
                }
            });
            for (int j = 0; j < groups.length; j++) {
                FileStatus[] nodes = fs.listStatus(groups[j].getPath(), new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        String nodeName = path.getName();
                        try {
                            FragmentId.parse(nodeName);
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                });
                for (FileStatus node : nodes) {
                    if (isValidFragmentPath(fs, node.getPath())) {
                        result.add(node);
                    } else {
                        logger.warn("Invalid fragment path:" + node.getPath());
                    }

                }
            }
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (FileStatus fileStatus : result) {
                sb.append(fileStatus.getPath());
                sb.append(",  ");
            }
            logger.debug("Mapper splits are: " + sb.toString());
        }
        logger.info("Total input paths to process : " + result.size());
        return result;
    }

    private boolean isValidFragmentPath(FileSystem fs, Path path) throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);
        if (files == null) {
            logger.warn("Invalid fragment path:{}, empty folder", path);
            return false;
        }
        boolean hasDataFile = false;
        boolean hasMetaFile = false;
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            Path childPath = fileStatus.getPath();
            String name = childPath.getName();

            if (name.endsWith(Constants.DATA_FILE_SUFFIX)) {
                hasDataFile = true;
            } else if (name.endsWith(Constants.META_FILE_SUFFIX)) {
                hasMetaFile = true;
            } else {
                logger.warn("Contains invalid file {} in path {}", childPath, path);
            }
        }
        if (hasDataFile && hasMetaFile) {
            return true;
        } else {
            logger.warn("Invalid fragment path:{}, data file exists:{}, meta file exists:{}", path, hasDataFile,
                    hasMetaFile);
            return false;
        }
    }
}
