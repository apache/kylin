package com.kylinolap.common.util;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by honma on 10/17/14.
 *
 * Keep this test case to test basic java functionality
 */
public class BasicTest {
    @Test
    public void test() throws IOException {
        File a  = File.createTempFile("aaa","bbb");
        System.out.println(a.toString());
    }
}
