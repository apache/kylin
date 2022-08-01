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

// https://stackoverflow.com/questions/1492856/easy-way-of-running-the-same-junit-test-over-and-over
package org.apache.kylin.junit.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepeatRule implements TestRule {
    private static final Logger logger = LoggerFactory.getLogger(RepeatRule.class);

    private static class RepeatStatement extends Statement {
        private final Statement statement;
        private final int repeat;
        private final Description description;

        public RepeatStatement(Statement statement, Description description, int repeat) {
            this.statement = statement;
            this.description = description;
            this.repeat = repeat;
        }

        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < repeat - 1; i++) {
                try {
                    statement.evaluate();
                    return;
                } catch (Throwable e) {
                    logger.warn("[UNSTABLE]" + description.getClassName() + "#" + description.getMethodName() + " fail "
                            + (i + 1) + " times", e);
                }
            }
            statement.evaluate();
        }

    }

    @Override
    public Statement apply(Statement statement, Description description) {
        Statement result = statement;
        Repeat repeat = description.getAnnotation(Repeat.class);
        if (repeat != null) {
            int times = repeat.value();
            result = new RepeatStatement(statement, description, times);
        }
        return result;
    }
}
