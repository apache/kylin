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
package org.apache.kylin.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.TreeMap;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class DumpHadoopSystemPropsTest extends AbstractTestCase {

    @Test
    public void testDumpProps() throws Exception {
        // test diffSystemProps
        Method diffSystemProps = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemProps", String.class);
        Unsafe.changeAccessibleObject(diffSystemProps, true);
        // for case same key and value exists in system props and the props file
        overwriteSystemProp("mapreduce.combine.class", "myclass");
        // for case same key but different value
        overwriteSystemProp("mapred.same.key", "value1");
        // for case property only exists in system props
        overwriteSystemProp("mapreduce.system.unique", "test");

        String propsURL = mockTempPropsFile().getAbsolutePath();
        TreeMap<String, String> propsMap = (TreeMap<String, String>) diffSystemProps.invoke(new DumpHadoopSystemProps(),
                propsURL);
        Assert.assertEquals(2, propsMap.size());
        Assert.assertEquals("mapred.same.key", propsMap.firstKey());
        Assert.assertEquals("value2", propsMap.firstEntry().getValue());
        Assert.assertEquals("10", propsMap.get("mapred.tip.id"));

        // test diffSystemEnvs
        Method diffSystemEnvs = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemEnvs", String.class);
        Unsafe.changeAccessibleObject(diffSystemEnvs, true);
        String envsURL = mockTempEnvFile().getAbsolutePath();
        TreeMap<String, String> envsMap = (TreeMap<String, String>) diffSystemEnvs.invoke(new DumpHadoopSystemProps(),
                envsURL);
        Assert.assertEquals(1, envsMap.size());
        Assert.assertEquals("mapred.map.child.env", envsMap.firstKey());
        Assert.assertEquals("myenv", envsMap.firstEntry().getValue());

        // test output() function
        Method output = DumpHadoopSystemProps.class.getDeclaredMethod("output", TreeMap.class, TreeMap.class,
                File.class);
        Unsafe.changeAccessibleObject(output, true);
        File tempFile = File.createTempFile("systemProps", ".tmp");
        try (InputStream in = new FileInputStream(tempFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()))) {
            output.invoke(new DumpHadoopSystemProps(), propsMap, envsMap, tempFile);
            Assert.assertEquals("export mapred.map.child.env=myenv", br.readLine());
            Assert.assertEquals("export kylin_hadoop_opts=\" -Dmapred.same.key=value2  -Dmapred.tip.id=10 \"",
                    br.readLine());
            Assert.assertEquals("rm -f " + tempFile.getAbsolutePath(), br.readLine());
        }
    }

    private File mockTempPropsFile() throws IOException {
        File propsFile = File.createTempFile("test", ".props");
        try (OutputStream os = new FileOutputStream(propsFile);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            writer.write("mapreduce.combine.class=myclass");
            writer.newLine();
            writer.write("mapred.same.key=value2");
            writer.newLine();
            // property only exists in props file will be exported
            writer.write("mapred.tip.id=10");
        }
        return propsFile;
    }

    private File mockTempEnvFile() throws IOException {
        File envFile = File.createTempFile("test", ".envs");
        try (OutputStream os = new FileOutputStream(envFile);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            writer.write("mapred.map.child.env=myenv");
        }
        return envFile;
    }
}
