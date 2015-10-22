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

package org.apache.kylin.cube.kv;

public class RowConstants {

    // row key fixed length place holder
    public static final byte ROWKEY_PLACE_HOLDER_BYTE = 9;
    // row key lower bound
    public static final byte ROWKEY_LOWER_BYTE = 0;
    // row key upper bound
    public static final byte ROWKEY_UPPER_BYTE = (byte) 0xff;

    // row key cuboid id length
    public static final int ROWKEY_CUBOIDID_LEN = 8;
    // row key shard length
    public static final int ROWKEY_SHARDID_LEN = 2;

    public static final int ROWKEY_HEADER_LEN = ROWKEY_CUBOIDID_LEN + ROWKEY_SHARDID_LEN;
    
    public static final byte BYTE_ZERO = 0;
    public static final byte BYTE_ONE = 1;

    // row value delimiter
    public static final byte ROWVALUE_DELIMITER_BYTE = 7;
    public static final String ROWVALUE_DELIMITER_STRING = String.valueOf((char) 7);
    public static final byte[] ROWVALUE_DELIMITER_BYTES = { 7 };

    public static final int ROWKEY_BUFFER_SIZE = 1024 * 1024; // 1 MB
    public static final int ROWVALUE_BUFFER_SIZE = 1024 * 1024; // 1 MB

    // marker class
    public static final byte[][] BYTE_ARR_MARKER = new byte[0][];

}
