package org.apache.kylin.common.util;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.junit.Test;

public class MemoryBudgetControllerTest {

    @Test
    public void test() {
        int n = MemoryBudgetController.getSystemAvailMB() / 2;
        MemoryBudgetController mbc = new MemoryBudgetController(n);

        ArrayList<OneMB> mbList = new ArrayList<OneMB>();
        for (int i = 0; i < n; i++) {
            mbList.add(new OneMB(mbc));
            assertEquals(mbList.size(), mbc.getTotalReservedMB());
        }

        mbc.reserve(new OneMB(), n);

        for (int i = 0; i < n; i++) {
            assertEquals(null, mbList.get(i).data);
        }

        try {
            mbc.reserve(new OneMB(), 1);
            fail();
        } catch (IllegalStateException ex) {
            // expected
        }
    }

    class OneMB implements MemoryBudgetController.MemoryConsumer {

        byte[] data;

        OneMB() {
        }

        OneMB(MemoryBudgetController mbc) {
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
