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

package org.apache.kylin.rest.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class ControllerSplitter {

    static File v1dir = new File("src/main/java/org/apache/kylin/rest/controller");
    static File v2dir = new File("src/main/java/org/apache/kylin/rest/controller2");
    static boolean dryRun = false;
    
    public static void main(String[] args) throws IOException {
        
        for (File f : v1dir.listFiles()) {
            chopOff(f, "application/vnd.apache.kylin-v2+json");
        }

        for (File f : v2dir.listFiles()) {
            chopOff(f, "application/json");
        }
    }

    private static void chopOff(File f, String annoPtn) throws IOException {
        
        System.out.println("Processing " + f);
        
        FileInputStream is = new FileInputStream(f);
        List<String> lines = IOUtils.readLines(is, "UTF-8");
        is.close();
        List<String> outLines = new ArrayList<>(lines.size());
        
        boolean del = false;
        for (String l : lines) {
            if (l.startsWith("    @") && l.contains(annoPtn))
                del = true;
            
            if (del)
                System.out.println("x " + l);
            else
                outLines.add(l);
            
            if (del && l.startsWith("    }"))
                del = false;
        }
        
        if (!dryRun && outLines.size() < lines.size()) {
            FileOutputStream os = new FileOutputStream(f);
            IOUtils.writeLines(outLines, "\n", os, "UTF-8");
            os.close();
            System.out.println("UPDATED " + f);
        } else {
            System.out.println("skipped");
        }
        
        System.out.println("============================================================================");
    }
}
