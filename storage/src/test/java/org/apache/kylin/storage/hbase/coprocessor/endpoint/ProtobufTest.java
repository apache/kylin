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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.apache.kylin.storage.hbase.coprocessor.endpoint.example.generated.NodeProtos;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by honma on 11/6/14.
 */
public class ProtobufTest {

    @Test
    public void testRecursive() throws IOException {

        NodeProtos.Node node = NodeProtos.Node.newBuilder().setName("root").setLeft(NodeProtos.Node.newBuilder().setName("L1 left").setLeft(NodeProtos.Node.newBuilder().setName("L2 left most"))).setRight(NodeProtos.Node.newBuilder().setName("L1 right").setRight(NodeProtos.Node.newBuilder().setName("L2 right most"))).build();

        File a = File.createTempFile("dfsd", "fdsfsd");
        FileOutputStream outputStream = new FileOutputStream(a);
        node.writeTo(outputStream);
        outputStream.close();

        FileInputStream inputStream = new FileInputStream(a);
        NodeProtos.Node newNode = NodeProtos.Node.parseFrom(inputStream);
        Assert.assertEquals(newNode.getLeft().getLeft().getName(), "L2 left most");

    }
}
