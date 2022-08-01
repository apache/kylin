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

package org.apache.kylin.measure.bitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

public class BitmapAggregatorTest {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;

    @Test
    public void testAggregator() {
        BitmapAggregator aggregator = new BitmapAggregator();
        assertNull(null, aggregator.getState());

        aggregator.aggregate(factory.newBitmap(10, 20, 30, 40));
        assertEquals(4, aggregator.getState().getCount());

        aggregator.aggregate(factory.newBitmap(25, 30, 35, 40, 45));
        assertEquals(7, aggregator.getState().getCount());

        aggregator.reset();
        assertNull(aggregator.getState());
    }

    @Test
    public void testAggregatorDeserializedCounter() throws IOException {
        BitmapCounter counter1 = factory.newBitmap(1, 3, 5);
        BitmapCounter counter2 = factory.newBitmap(1, 2, 4, 6);
        BitmapCounter counter3 = factory.newBitmap(1, 5, 7);

        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
        counter1.write(buffer);
        counter2.write(buffer);
        counter3.write(buffer);
        buffer.flip();

        BitmapAggregator aggregator = new BitmapAggregator();

        // first
        BitmapCounter next = factory.newBitmap(buffer);
        assertEquals(counter1, next);

        aggregator.aggregate(next);
        assertEquals(counter1, aggregator.getState());

        // second
        next = factory.newBitmap(buffer);
        assertEquals(counter2, next);

        aggregator.aggregate(next);
        assertEquals(6, aggregator.getState().getCount());

        // third
        next = factory.newBitmap(buffer);
        assertEquals(counter3, next);

        aggregator.aggregate(next);
        assertEquals(7, aggregator.getState().getCount());

        BitmapCounter result = factory.newBitmap();
        result.orWith(counter1);
        result.orWith(counter2);
        result.orWith(counter3);
        assertEquals(result, aggregator.getState());

    }
}
