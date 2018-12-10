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

package org.apache.kylin.stream.core.storage.columnar.invertindex;//package org.apache.kylin.stream.invertindex;
//
//import java.util.List;
//import java.util.Random;
//
//import org.apache.kylin.common.util.Bytes;
//import org.junit.Before;
//import org.junit.Test;
//
///**
// *
// */
//public class ColInvertIndexWriterWriterTest {
//    FixLenColInvertIndexWriter cii;
//    String indexFolder;
//
//    @Before
//    public void setup(){
//        cii = new FixLenColInvertIndexWriter("column1",4);
//        indexFolder = ".";
//    }
//
//    @Test
//    public void testSearch() throws Exception{
//        int size = 100;
//        Random r = new Random();
//        for (int i =0;i<size;i++){
//            cii.addValue(Bytes.toBytes(i % 20));
//        }
////        List<Integer> rows = cii.searchValue(10);
////        System.out.println(rows);
//    }
//
//    @Test
//    public void testFlush() throws Exception{
//        int size = 5000000;
//        Random r = new Random();
//        long addStart = System.currentTimeMillis();
//        for (int i =0;i<size;i++){
//            cii.addValue(Bytes.toBytes(r.nextInt(200000)));
//        }
//        long addEnd = System.currentTimeMillis();
//        System.out.println("add value takes:" + (addEnd-addStart));
////        cii.flush(indexFolder);
////        long flushEnd = System.currentTimeMillis();
////        System.out.println("flush takes:" + (flushEnd-addEnd));
//    }
//
//    @Test
//    public void testLoadAndSearch() throws Exception{
//        long loadStart = System.currentTimeMillis();
//        SimpleColInvertIndex loadedIdx = SimpleColInvertIndex.load(indexFolder,"column1");
//        long loadEnd = System.currentTimeMillis();
//        System.out.println("load takes:" + (loadEnd-loadStart));
//        List<Integer> result = loadedIdx.searchValue(17);
//        System.out.println("search takes:" + (System.currentTimeMillis()-loadEnd));
//        System.out.println("result size:" + result.size());
//        System.out.println(result);
//    }
//
//}
