package com.kylinolap.common.util;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;

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
    @Test
    public void test() throws IOException {
        for (String s : ManagementFactory.getRuntimeMXBean().getInputArguments())
            System.out.println(s);
        return;
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
