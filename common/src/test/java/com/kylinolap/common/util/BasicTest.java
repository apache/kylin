package com.kylinolap.common.util;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by honma on 10/17/14.
 * <p/>
 * Keep this test case to test basic java functionality
 * development concept proving use
 */
@Ignore("convenient trial tool for dev")
public class BasicTest {
    private void log(ByteBuffer a) {
        Integer x = 4;
        foo(x);
    }

    private void foo(Long a) {
        System.out.printf("a");

    }

    private void foo(Integer b) {
        System.out.printf("b");
    }

    @Test
    @Ignore("convenient trial tool for dev")
    public void test1() throws IOException {
        // only for trial
    }

    @Test
    @Ignore("fix it later")
    public void test2() throws IOException {
        // fix it later
    }

}
