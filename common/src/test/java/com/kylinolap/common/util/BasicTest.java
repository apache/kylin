package com.kylinolap.common.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.*;

/**
* Created by honma on 10/17/14.
* <p/>
* Keep this test case to test basic java functionality
* development concept proving use
*/
@Ignore("convenient trial tool for dev")
@SuppressWarnings("unused")
public class BasicTest {
    protected static final org.slf4j.Logger log = LoggerFactory.getLogger(BasicTest.class);
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
    public void test1() throws IOException, InterruptedException {
        String leftAlignFormat = "| %-100s | %n";

        System.out.format("+------------------------------------------------------------------------------------------------------+%n");
        System.out.format(leftAlignFormat, "aaaadfafdsadddaaaadfafdsaddddddddddddddddddaaaadfafdsaddddddddddddddddddaaaadfafdsaddddddddddddddddddaaaadfafdsaddddddddddddddddddaaaadfafdsaddddddddddddddddddaaaadfafdsaddddddddddddddddddddddddddddddddd");
        System.out.format("+------------------------------------------------------------------------------------------------------+%n");

    }

    @Test
    @Ignore("fix it later")
    public void test2() throws IOException {
    }
}
