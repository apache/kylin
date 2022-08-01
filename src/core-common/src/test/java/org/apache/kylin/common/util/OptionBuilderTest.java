/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.common.util;

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class OptionBuilderTest {

    @Test
    public void testBasic() {
        String argName = "dir";
        String opt = "destDir";
        String desc = "specify the dest dir to save the related information";
        String longOpt = "dest_dir";
        char sep = ',';
        Option option = OptionBuilder.getInstance().withArgName(argName).hasArg().isRequired(true).withDescription(desc)
                .withLongOpt(longOpt).withType(OptionBuilder.class).withValueSeparator(sep).create(opt);

        Assert.assertTrue(option.isRequired());
        Assert.assertTrue(option.hasArg());
        Assert.assertEquals(opt, option.getOpt());
        Assert.assertEquals(argName, option.getArgName());
        Assert.assertEquals(desc, option.getDescription());
        Assert.assertEquals(sep, option.getValueSeparator());
        Assert.assertEquals(longOpt, option.getLongOpt());
        Assert.assertEquals(OptionBuilder.class, option.getType());

        Option optionFalse = OptionBuilder.getInstance().withArgName(argName).hasArg().isRequired(false)
                .withDescription(desc).withLongOpt(longOpt).withType(OptionBuilder.class).withValueSeparator(sep)
                .create(opt);
        Assert.assertFalse(optionFalse.isRequired());
    }

    @Test
    public void testConcurrent() {
        Thread t1 = new Thread(() -> {
            while (true) {
                org.apache.commons.cli.OptionBuilder.isRequired(false).create("falseOpt");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("t1 interrupt");
                }
            }
        });

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            t1.start();

            while (true) {
                Option option = org.apache.commons.cli.OptionBuilder.isRequired(true).create("trueOpt");
                if (!option.isRequired()) {
                    // concurrent issue
                    break;
                }
            }
            t1.interrupt();
        });

        Thread t2 = new Thread(() -> {
            while (true) {
                OptionBuilder.getInstance().isRequired(false).create("falseOpt");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("t2 interrupt");
                }
            }
        });

        t2.start();

        for (int i = 0; i < 10000; i++) {
            Option option = OptionBuilder.getInstance().isRequired(true).create("trueOpt");
            Assert.assertTrue(option.isRequired());
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.out.println("t2 interrupt");
            }
        }

        t2.interrupt();
    }

}
