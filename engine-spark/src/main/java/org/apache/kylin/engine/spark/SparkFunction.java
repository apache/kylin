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

package org.apache.kylin.engine.spark;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

public class SparkFunction {

    protected static final Logger logger = LoggerFactory.getLogger(SparkFunction.class);

    private static abstract class FunctionBase implements Serializable {
        private volatile transient boolean initialized = false;
        private transient int recordCounter;

        protected abstract void doInit();

        protected void init() {
            if (!initialized) {
                synchronized (SparkFunction.class) {
                    if (!initialized) {
                        logger.info("Start to do init for {}", this);
                        doInit();
                        initialized = true;
                        recordCounter = 0;
                    }
                }
            }
            if (recordCounter++ % SparkUtil.getNormalRecordLogThreshold() == 0) {
                logger.info("Accepting record with ordinal: " + recordCounter);
                logger.info("Do call, available memory: {}m", MemoryBudgetController.getSystemAvailMB());
            }
        }
    }

    public static abstract class PairFunctionBase<T, K, V> extends FunctionBase implements PairFunction<T, K, V> {

        protected abstract Tuple2<K, V> doCall(T t) throws Exception;

        @Override
        public Tuple2<K, V> call(T t) throws Exception {
            init();
            return doCall(t);
        }
    }

    public static abstract class Function2Base<T1, T2, R> extends FunctionBase implements Function2<T1, T2, R> {

        protected abstract R doCall(T1 v1, T2 v2) throws Exception;

        @Override
        public R call(T1 v1, T2 v2) throws Exception {
            init();
            return doCall(v1, v2);
        }
    }

    public static abstract class PairFlatMapFunctionBase<T, K, V> extends FunctionBase implements PairFlatMapFunction<T, K, V> {

        protected abstract Iterator<Tuple2<K, V>> doCall(T t) throws Exception;

        @Override
        public Iterator<Tuple2<K, V>> call(T t) throws Exception {
            init();
            return doCall(t);
        }
    }

    public static abstract class VoidFunctionBase<T> extends FunctionBase implements VoidFunction<T> {

        protected abstract void doCall(T t) throws Exception;

        @Override
        public void call(T t) throws Exception {
            init();
            doCall(t);
        }
    }
}
