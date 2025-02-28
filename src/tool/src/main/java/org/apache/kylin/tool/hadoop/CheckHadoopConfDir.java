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

package org.apache.kylin.tool.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.Unsafe;

public class CheckHadoopConfDir {
    public static final String CHECKENV_REPORT_PFX = ">   ";

    public static void main(String[] args) throws Exception {

        if (1 != args.length) {
            usage();
            Unsafe.systemExit(1);
        }

        File hadoopConfDir = new File(args[0]).getCanonicalFile();

        System.out.println("Checking hadoop config dir " + hadoopConfDir);

        if (!hadoopConfDir.exists()) {
            System.err.println("ERROR: Hadoop config dir '" + hadoopConfDir + "' does not exist");
            Unsafe.systemExit(1);
        }

        if (!hadoopConfDir.isDirectory()) {
            System.err.println("ERROR: Hadoop config dir '" + hadoopConfDir + "' is not a directory");
            Unsafe.systemExit(1);
        }

        LocalFileSystem localfs = getLocalFSAndHitUGIForTheFirstTime();

        // don't load defaults, we are only interested in the specified config dir
        Configuration conf = new Configuration(false);
        for (File f : Objects.requireNonNull(hadoopConfDir.listFiles())) {
            if (f.getName().endsWith("-site.xml")) {
                Path p = new Path(f.toString());
                assert localfs != null;
                p = localfs.makeQualified(p);
                conf.addResource(p);
                System.out.println("Load " + p);
            }
        }
        conf.reloadConfiguration();

        boolean shortcircuit = conf.getBoolean("dfs.client.read.shortcircuit", false);
        if (!shortcircuit) {
            System.out.println(CHECKENV_REPORT_PFX
                    + "WARN: 'dfs.client.read.shortcircuit' is not enabled which could impact query performance. Check "
                    + hadoopConfDir + "/hdfs-site.xml");
        }

        Unsafe.systemExit(0);
    }

    /*
     * Although this is getting a LocalFileSystem, but it triggers Hadoop security check inside.
     * This is the very first time we hit UGI during the check-env process, 
     * and could hit Kerberos exception in a secured Hadoop.
     * Be careful about the error reporting.
     */
    private static LocalFileSystem getLocalFSAndHitUGIForTheFirstTime() {
        try {
            return FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            System.err.println(
                    "ERROR: Hadoop security exception? Seems the classpath is not setup propertly regarding Hadoop security.");
            System.err.println("Detailed error message: " + e.getMessage());
            Unsafe.systemExit(1);
            return null;
        }
    }

    private static void usage() {
        System.out.println("Usage: CheckHadoopConfDir hadoopConfDir");
    }
}
