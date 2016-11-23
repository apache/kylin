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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

/**
 * @author yangli9
 * 
 */
public class TableReaderTest {

    @Test
    public void testBasicReader() throws IOException {
        File f = new File("src/test/resources/dict/DW_SITES");
        DFSFileTableReader reader = new DFSFileTableReader("file://" + f.getAbsolutePath(), DFSFileTable.DELIM_AUTO, 10);
        while (reader.next()) {
            assertEquals("[-1, Korea Auction.co.kr, S, 48, 0, 111, 2009-02-11, , DW_OFFPLAT, ]", Arrays.toString(reader.getRow()));
            break;
        }
        reader.close();

    }
}
