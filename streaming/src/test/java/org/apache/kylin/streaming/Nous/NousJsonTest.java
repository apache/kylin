package org.apache.kylin.streaming.Nous;

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.Test;

/**
 * Created by Hongbin Ma(Binmahone) on 3/16/15.
 */
public class NousJsonTest {
    @Test
    public void foo() throws IOException {
        NousMessage a = new NousMessage(1, 2, "a", "b", "c", "d", "e", 100, 200.0, 300);
        String x = JsonUtil.writeValueAsIndentString(a);
        NousMessage b = JsonUtil.readValue(x,NousMessage.class);
        System.out.print(b.getClick());
    }
}
