package com.kylinolap.common.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by honma on 10/17/14.
 * <p/>
 * Keep this test case to test basic java functionality
 * development concept proving use
 */
@Ignore
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
    public void test() throws IOException {
        Set<String> a = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        a.add("aaa");
        System.out.println(a.contains("AaA"));
        //        for (String s : ManagementFactory.getRuntimeMXBean().getInputArguments())
//            System.out.println(s);
//        return;

//        //BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(1000000);
//        BlockingQueue<Runnable> workQueue = new SynchronousQueue<>();
//
//        ExecutorService pool = new ThreadPoolExecutor(1, 1000, 60, TimeUnit.SECONDS,
//                workQueue, Threads.newDaemonThreadFactory("htable"));
//
//        for (int i = 0; i < 10000; ++i) {
//            pool.submit(new Callable<Object>() {
//                @Override
//                public Object call() throws Exception {
//                    Thread.sleep(1000);
//                }
//            });
//        }


//
//        for (int i = 0; i < 10000; ++i) {
//            System.out.println("Hello from a thread! " + i);
//            new Thread() {
//                @Override
//                public void run() {
//                    try {
//                        Thread.sleep(1000000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }.start();
//        }


    }


}
