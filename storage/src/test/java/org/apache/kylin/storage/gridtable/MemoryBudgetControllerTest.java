package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.kylin.storage.gridtable.memstore.MemoryBudgetController;
import org.apache.kylin.storage.gridtable.memstore.MemoryBudgetController.NotEnoughBudgetException;
import org.junit.Test;

public class MemoryBudgetControllerTest {

    @Test
    public void test() {
        final int n = MemoryBudgetController.getSystemAvailMB() / 2;
        final MemoryBudgetController mbc = new MemoryBudgetController(n);

        ArrayList<Consumer> mbList = new ArrayList<Consumer>();
        for (int i = 0; i < n; i++) {
            mbList.add(new Consumer(mbc));
            assertEquals(mbList.size(), mbc.getTotalReservedMB());
        }

        // a's reservation will free up all the previous
        final Consumer a = new Consumer();
        mbc.reserve(a, n);
        for (int i = 0; i < n; i++) {
            assertEquals(null, mbList.get(i).data);
        }
        
        // cancel a in 2 seconds
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                mbc.reserve(a, 0);
            }
        }.start();
        
        // b will success after some wait
        long bWaitStart = System.currentTimeMillis();
        final Consumer b = new Consumer();
        mbc.reserveInsist(b, n);
        assertTrue(System.currentTimeMillis() - bWaitStart > 1000);

        try {
            mbc.reserve(a, 1);
            fail();
        } catch (NotEnoughBudgetException ex) {
            // expected
        }
    }

    class Consumer implements MemoryBudgetController.MemoryConsumer {

        byte[] data;

        Consumer() {
        }

        Consumer(MemoryBudgetController mbc) {
            mbc.reserve(this, 1);
            data = new byte[MemoryBudgetController.ONE_MB - 24]; // 24 is object shell of this + object shell of data + reference of data 
        }

        @Override
        public int freeUp(int mb) {
            if (data != null) {
                data = null;
                return 1;
            } else {
                return 0;
            }
        }

    }
}
