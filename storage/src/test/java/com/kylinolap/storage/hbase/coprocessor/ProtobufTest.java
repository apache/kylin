package com.kylinolap.storage.hbase.coprocessor;

import com.kylinolap.storage.hbase.coprocessor.example.generated.NodeProtos;
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


        NodeProtos.Node node = NodeProtos.Node.newBuilder().setName("root").
                setLeft(
                        NodeProtos.Node.newBuilder().setName("L1 left").
                                setLeft(
                                        NodeProtos.Node.newBuilder().setName("L2 left most"))).
                setRight(
                        NodeProtos.Node.newBuilder().setName("L1 right").
                                setRight(
                                        NodeProtos.Node.newBuilder().setName("L2 right most"))).build();

        File a = File.createTempFile("dfsd", "fdsfsd");
        FileOutputStream outputStream = new FileOutputStream(a);
        node.writeTo(outputStream);
        outputStream.close();

        FileInputStream inputStream = new FileInputStream(a);
        NodeProtos.Node newNode = NodeProtos.Node.parseFrom(inputStream);
        Assert.assertEquals(newNode.getLeft().getLeft().getName(), "L2 left most");

    }
}
