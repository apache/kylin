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

package org.apache.kylin.job.hadoop.invertedindex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THIS IS A TAILORED DUPLICATE OF org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI TO AVOID CYCLIC
 * DEPENDENCY. INVERTED-INDEX CODE NOW SPLITTED ACROSS kylin-invertedindex AND kylin-storage-hbase.
 * DEFENITELY NEED FURTHER REFACTOR.
 */
public class IIDeployCoprocessorCLI {

    private static final Logger logger = LoggerFactory.getLogger(IIDeployCoprocessorCLI.class);

    public static final String CubeObserverClass = "org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.AggregateRegionObserver";
    public static final String CubeEndpointClass = "org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.CubeVisitService";
    public static final String IIEndpointClass = "org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.IIEndpoint";

    public static void deployCoprocessor(HTableDescriptor tableDesc) {
        try {
            initHTableCoprocessor(tableDesc);
            logger.info("hbase table " + tableDesc.getName() + " deployed with coprocessor.");

        } catch (Exception ex) {
            logger.error("Error deploying coprocessor on " + tableDesc.getName(), ex);
            logger.error("Will try creating the table without coprocessor.");
        }
    }

    private static void initHTableCoprocessor(HTableDescriptor desc) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Configuration hconf = HadoopUtil.getCurrentConfiguration();
        FileSystem fileSystem = FileSystem.get(hconf);

        String localCoprocessorJar = kylinConfig.getCoprocessorLocalJar();
        Path hdfsCoprocessorJar = uploadCoprocessorJar(localCoprocessorJar, fileSystem, null);

        addCoprocessorOnHTable(desc, hdfsCoprocessorJar);
    }

    private static void addCoprocessorOnHTable(HTableDescriptor desc, Path hdfsCoprocessorJar) throws IOException {
        logger.info("Add coprocessor on " + desc.getNameAsString());
        desc.addCoprocessor(IIEndpointClass, hdfsCoprocessorJar, 1000, null);
        desc.addCoprocessor(CubeEndpointClass, hdfsCoprocessorJar, 1001, null);
        desc.addCoprocessor(CubeObserverClass, hdfsCoprocessorJar, 1002, null);
    }

    private static Path uploadCoprocessorJar(String localCoprocessorJar, FileSystem fileSystem, Set<String> oldJarPaths) throws IOException {
        Path uploadPath = null;
        File localCoprocessorFile = new File(localCoprocessorJar);

        // check existing jars
        if (oldJarPaths == null) {
            oldJarPaths = new HashSet<String>();
        }
        Path coprocessorDir = getCoprocessorHDFSDir(fileSystem, KylinConfig.getInstanceFromEnv());
        for (FileStatus fileStatus : fileSystem.listStatus(coprocessorDir)) {
            if (isSame(localCoprocessorFile, fileStatus)) {
                uploadPath = fileStatus.getPath();
                break;
            }
            String filename = fileStatus.getPath().toString();
            if (filename.endsWith(".jar")) {
                oldJarPaths.add(filename);
            }
        }

        // upload if not existing
        if (uploadPath == null) {
            // figure out a unique new jar file name
            Set<String> oldJarNames = new HashSet<String>();
            for (String path : oldJarPaths) {
                oldJarNames.add(new Path(path).getName());
            }
            String baseName = getBaseFileName(localCoprocessorJar);
            String newName = null;
            int i = 0;
            while (newName == null) {
                newName = baseName + "-" + (i++) + ".jar";
                if (oldJarNames.contains(newName))
                    newName = null;
            }

            // upload
            uploadPath = new Path(coprocessorDir, newName);
            FileInputStream in = null;
            FSDataOutputStream out = null;
            try {
                in = new FileInputStream(localCoprocessorFile);
                out = fileSystem.create(uploadPath);
                IOUtils.copy(in, out);
            } finally {
                IOUtils.closeQuietly(in);
                IOUtils.closeQuietly(out);
            }

            fileSystem.setTimes(uploadPath, localCoprocessorFile.lastModified(), -1);

        }

        uploadPath = uploadPath.makeQualified(fileSystem.getUri(), null);
        return uploadPath;
    }

    private static boolean isSame(File localCoprocessorFile, FileStatus fileStatus) {
        return fileStatus.getLen() == localCoprocessorFile.length() && fileStatus.getModificationTime() == localCoprocessorFile.lastModified();
    }

    private static String getBaseFileName(String localCoprocessorJar) {
        File localJar = new File(localCoprocessorJar);
        String baseName = localJar.getName();
        if (baseName.endsWith(".jar"))
            baseName = baseName.substring(0, baseName.length() - ".jar".length());
        return baseName;
    }

    private static Path getCoprocessorHDFSDir(FileSystem fileSystem, KylinConfig config) throws IOException {
        String hdfsWorkingDirectory = config.getHdfsWorkingDirectory();
        Path coprocessorDir = new Path(hdfsWorkingDirectory, "coprocessor");
        fileSystem.mkdirs(coprocessorDir);
        return coprocessorDir;
    }

}
