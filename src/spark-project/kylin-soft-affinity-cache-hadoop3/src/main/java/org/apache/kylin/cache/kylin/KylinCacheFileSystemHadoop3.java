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

package org.apache.kylin.cache.kylin;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinCacheFileSystemHadoop3 extends KylinCacheFileSystem {

    @Override
    public FutureDataInputStreamBuilder openFile(final Path path) throws UnsupportedOperationException {
        return new FutureDataInputStreamBuilderImpl(fs, path) {
            @Override
            public CompletableFuture<FSDataInputStream> build() throws IllegalArgumentException, UnsupportedOperationException, IOException {
                return CompletableFuture.completedFuture(KylinCacheFileSystemHadoop3.this.open(path));
            }
        };
    }
}
