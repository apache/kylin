package org.apache.kylin.common.util;


import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Hongbin Ma(Binmahone) on 12/31/14.
 */
public class ThreadUtil {
    @SuppressWarnings("unused")
    public static void main(String[] args) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());//Threads.newDaemonThreadFactory("htable"));

        for (int i = 0; i < Integer.MAX_VALUE; ++i) {
            System.out.println("index: " + i);
            Future<?> future = pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
