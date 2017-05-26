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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;

/**
 */
public class InstallJarIntoMavenTest {

    @Ignore("convenient trial tool for dev")
    public void testInstall() throws IOException {
        File folder = new File("/export/home/b_kylin/tmp");
        File out = new File("/export/home/b_kylin/tmp/out.sh");
        out.createNewFile();
        FileWriter fw = new FileWriter(out);

        for (File file : folder.listFiles()) {
            String name = file.getName();

            if (!name.endsWith(".jar"))
                continue;

            int firstSlash = name.indexOf('-');
            int lastDot = name.lastIndexOf('.');
            String groupId = name.substring(0, firstSlash);

            Pattern pattern = Pattern.compile("-\\d");
            Matcher match = pattern.matcher(name);
            match.find();
            String artifactId = name.substring(0, match.start());
            String version = name.substring(match.start() + 1, lastDot);

            fw.write(String.format("mvn install:install-file -Dfile=%s -DgroupId=%s -DartifactId=%s -Dversion=%s -Dpackaging=jar", name, "org.apache." + groupId, artifactId, version));
            fw.write("\n");
        }
        fw.close();
    }

}
