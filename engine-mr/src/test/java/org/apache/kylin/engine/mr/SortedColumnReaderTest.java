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
package org.apache.kylin.engine.mr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.UUID;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.dict.ByteComparator;
import org.apache.kylin.dict.BytesConverter;
import org.apache.kylin.dict.IDictionaryValueEnumerator;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TableColumnValueEnumerator;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by xiefan on 16-11-14.
 */
public class SortedColumnReaderTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testReadStringMultiFile() throws Exception {
        String dirPath = "src/test/resources/multi_file_str";
        ArrayList<String> correctAnswer = readAllFiles(dirPath);
        Collections.sort(correctAnswer, new ByteComparator<String>(new StringBytesConverter()));
        SortedColumnDFSFile column = new SortedColumnDFSFile(qualify(dirPath + "/"), DataType.getType("varchar"));
        IDictionaryValueEnumerator e = new TableColumnValueEnumerator(column.getReader(), -1);
        ArrayList<String> output = new ArrayList<>();
        while (e.moveNext()) {
            output.add(new String(e.current()));
        }
        System.out.println(correctAnswer.size());
        assertTrue(correctAnswer.size() == output.size());
        for (int i = 0; i < correctAnswer.size(); i++) {
            assertEquals(correctAnswer.get(i), output.get(i));
        }
    }

    @Ignore
    @Test
    public void createStringTestFiles() throws Exception {
        String dirPath = "src/test/resources/multi_file_str";
        String prefix = "src/test/resources/multi_file_str/data_";
        ArrayList<String> data = new ArrayList<>();
        int num = 10000;
        for (int i = 0; i < num; i++) {
            UUID uuid = RandomUtil.randomUUID();
            data.add(uuid.toString());
        }
        Collections.sort(data, new ByteComparator<String>(new StringBytesConverter()));
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<File> allFiles = new ArrayList<>();
        int fileNum = 5;
        for (int i = 0; i < fileNum; i++) {
            File f = new File(prefix + i);
            if (!f.exists())
                f.createNewFile();
            allFiles.add(f);
        }
        ArrayList<BufferedWriter> bws = new ArrayList<>();
        for (File f : allFiles) {
            bws.add(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)));
        }
        System.out.println(data.size());
        for (String str : data) {
            int fileId = rand.nextInt(fileNum);
            BufferedWriter bw = bws.get(fileId);
            bw.write(str);
            bw.newLine();
        }
        for (BufferedWriter bw : bws) {
            bw.flush();
            bw.close();
        }
        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        for (File file : files) {
            System.out.println("file:" + file.getAbsolutePath() + " size:" + file.length());
        }
    }

    @Test
    public void testReadIntegerMultiFiles() throws Exception {
        String dirPath = "src/test/resources/multi_file_int";
        ArrayList<String> correctAnswer = readAllFiles(dirPath);
        Collections.sort(correctAnswer, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                try {
                    Long l1 = Long.parseLong(o1);
                    Long l2 = Long.parseLong(o2);
                    return l1.compareTo(l2);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    return 0;
                }
            }
        });
        SortedColumnDFSFile column = new SortedColumnDFSFile(qualify(dirPath + "/"), DataType.getType("long"));
        IDictionaryValueEnumerator e = new TableColumnValueEnumerator(column.getReader(), -1);
        ArrayList<String> output = new ArrayList<>();
        while (e.moveNext()) {
            output.add(new String(e.current()));
        }
        System.out.println(correctAnswer.size());
        assertTrue(correctAnswer.size() == output.size());
        for (int i = 0; i < correctAnswer.size(); i++) {
            assertEquals(correctAnswer.get(i), output.get(i));
        }
    }

    @Test
    public void testEmptyDir() throws Exception {
        String dirPath = "src/test/resources/empty_dir";
        new File(dirPath).mkdirs();
        SortedColumnDFSFile column = new SortedColumnDFSFile(qualify(dirPath + "/"), DataType.getType("varchar"));
        IDictionaryValueEnumerator e = new TableColumnValueEnumerator(column.getReader(), -1);
        ArrayList<String> output = new ArrayList<>();
        while (e.moveNext()) {
            System.out.println(new String(e.current()));
            output.add(new String(e.current()));
        }
        System.out.println(output.size());
    }

    @Test
    public void testEmptyFile() throws Exception {
        String dirPath = "src/test/resources/multi_file_empty_file";
        ArrayList<String> correctAnswer = readAllFiles(dirPath);
        final BytesConverter<String> converter = new StringBytesConverter();
        Collections.sort(correctAnswer, new ByteComparator<String>(new StringBytesConverter()));
        System.out.println("correct answer:" + correctAnswer);
        SortedColumnDFSFile column = new SortedColumnDFSFile(qualify(dirPath + "/"), DataType.getType("varchar"));
        IDictionaryValueEnumerator e = new TableColumnValueEnumerator(column.getReader(), -1);
        ArrayList<String> output = new ArrayList<>();
        while (e.moveNext()) {
            output.add(new String(e.current()));
        }
        System.out.println(correctAnswer.size());
        assertTrue(correctAnswer.size() == output.size());
        for (int i = 0; i < correctAnswer.size(); i++) {
            assertEquals(correctAnswer.get(i), output.get(i));
        }
    }

    @Ignore
    @Test
    public void createIntegerTestFiles() throws Exception {
        String dirPath = "src/test/resources/multi_file_int";
        String prefix = "src/test/resources/multi_file_int/data_";
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> data = new ArrayList<>();
        int num = 10000;
        for (int i = 0; i < num; i++) {
            data.add(i + "");
        }
        ArrayList<File> allFiles = new ArrayList<>();
        int fileNum = 5;
        for (int i = 0; i < fileNum; i++) {
            File f = new File(prefix + i);
            if (!f.exists())
                f.createNewFile();
            allFiles.add(f);
        }
        ArrayList<BufferedWriter> bws = new ArrayList<>();
        for (File f : allFiles) {
            bws.add(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)));
        }
        System.out.println(data.size());
        for (String str : data) {
            int fileId = rand.nextInt(fileNum);
            BufferedWriter bw = bws.get(fileId);
            bw.write(str);
            bw.newLine();
        }
        for (BufferedWriter bw : bws) {
            bw.flush();
            bw.close();
        }
        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        for (File file : files) {
            System.out.println("file:" + file.getAbsolutePath() + " size:" + file.length());
        }
    }

    @Test
    public void testReadDoubleMultiFiles() throws Exception {
        String dirPath = "src/test/resources/multi_file_double";
        ArrayList<String> correctAnswer = readAllFiles(dirPath);
        Collections.sort(correctAnswer, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                try {
                    Double d1 = Double.parseDouble(o1);
                    Double d2 = Double.parseDouble(o2);
                    return d1.compareTo(d2);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    return 0;
                }
            }
        });
        SortedColumnDFSFile column = new SortedColumnDFSFile(qualify(dirPath + "/"), DataType.getType("double"));
        IDictionaryValueEnumerator e = new TableColumnValueEnumerator(column.getReader(), -1);
        ArrayList<String> output = new ArrayList<>();
        while (e.moveNext()) {
            output.add(new String(e.current()));
        }
        System.out.println(correctAnswer.size());
        assertTrue(correctAnswer.size() == output.size());
        for (int i = 0; i < correctAnswer.size(); i++) {
            assertEquals(correctAnswer.get(i), output.get(i));
        }
    }

    @Ignore
    @Test
    public void createDoubleTestFiles() throws Exception {
        String dirPath = "src/test/resources/multi_file_double";
        String prefix = "src/test/resources/multi_file_double/data_";
        Random rand = new Random(System.currentTimeMillis());
        ArrayList<String> data = new ArrayList<>();
        int num = 10000;
        double k = 0.0;
        for (int i = 0; i < num; i++) {
            data.add(k + "");
            k += 0.52;
        }
        ArrayList<File> allFiles = new ArrayList<>();
        int fileNum = 5;
        for (int i = 0; i < fileNum; i++) {
            File f = new File(prefix + i);
            if (!f.exists())
                f.createNewFile();
            allFiles.add(f);
        }
        ArrayList<BufferedWriter> bws = new ArrayList<>();
        for (File f : allFiles) {
            bws.add(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)));
        }
        System.out.println(data.size());
        for (String str : data) {
            int fileId = rand.nextInt(fileNum);
            BufferedWriter bw = bws.get(fileId);
            bw.write(str);
            bw.newLine();
        }
        for (BufferedWriter bw : bws) {
            bw.flush();
            bw.close();
        }
        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        for (File file : files) {
            System.out.println("file:" + file.getAbsolutePath() + " size:" + file.length());
        }
    }

    private ArrayList<String> readAllFiles(String dirPath) throws Exception {
        ArrayList<String> result = new ArrayList<>();
        File dir = new File(dirPath);
        for (File f : dir.listFiles()) {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8));
            String str = br.readLine();
            while (str != null) {
                result.add(str);
                str = br.readLine();
            }
        }
        return result;
    }

    private String qualify(String path) {
        String absolutePath = new File(path).getAbsolutePath();
        if (absolutePath.startsWith("/"))
            return "file://" + absolutePath;
        else
            return "file:///" + absolutePath;
    }

}
