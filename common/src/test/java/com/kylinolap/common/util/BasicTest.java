package com.kylinolap.common.util;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by honma on 10/17/14.
 *
 * Keep this test case to test basic java functionality
 * development concept proving use
 */
@Ignore
public class BasicTest {
    @Test
    public void test() throws IOException {
        double i2 = 3234.4324234324234;
        System.out.println(String.format("%.2f", i2));
    }
}
