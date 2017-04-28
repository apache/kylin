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
package org.apache.kylin.dict;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;

/**
 * Created by sunyerui on 16/11/15.
 */
public class AppendTrieDictionaryChecker {

    public boolean runChecker(String baseDir) throws IOException {
        Path basePath = new Path(baseDir);
        FileSystem fs = HadoopUtil.getFileSystem(basePath);
        List<Path> sliceList = new ArrayList<>();
        List<Path> corruptedSliceList = new ArrayList<>();
        listDictSlicePath(fs, fs.getFileStatus(basePath), sliceList);

        for (Path path : sliceList) {
            if (!doCheck(fs, path)) {
                System.out.println("AppendDict Slice " + path + " corrupted");
                corruptedSliceList.add(path);
            } else {
                System.out.println("AppendDict Slice " + path + " is right");
            }
        }

        if (corruptedSliceList.isEmpty()) {
            System.out.println("ALL AppendDict Slices is right");
            return true;
        } else {
            System.out.println("Some AppendDict Slice(s) corrupted: ");
            for (Path path : corruptedSliceList) {
                System.out.println(path.toString());
            }
            return false;
        }
    }

    public void listDictSlicePath(FileSystem fs, FileStatus path, List<Path> list) throws IOException {
        if (path.isDirectory()) {
            for (FileStatus status : fs.listStatus(path.getPath())) {
                listDictSlicePath(fs, status, list);
            }
        } else {
            if (path.getPath().getName().startsWith(CachedTreeMap.CACHED_PREFIX)) {
                list.add(path.getPath());
            }
        }
    }

    public boolean doCheck(FileSystem fs, Path filePath) {
        try (FSDataInputStream input = fs.open(filePath, CachedTreeMap.BUFFER_SIZE)) {
            AppendTrieDictionary.DictSlice slice = new AppendTrieDictionary.DictSlice();
            slice.readFields(input);
            return slice.doCheck();
        } catch (Exception e) {
            return false;
        } catch (Error e) {
            return false;
        }
    }

    public static void main(String[] args) throws IOException {
        String path = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/GlobalDict/";
        if (args.length > 0) {
            path = args[0];
        }
        System.out.println("Recursive Check AppendTrieDictionary Slices in path " + path);
        AppendTrieDictionaryChecker checker = new AppendTrieDictionaryChecker();
        if (checker.runChecker(path)) {
            System.exit(0);
        } else {
            System.exit(-1);
        }
    }
}
