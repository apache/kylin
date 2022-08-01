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

package org.apache.kylin.junit.rule;

import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;

import lombok.experimental.Delegate;

public class TransactionExceptedException implements TestRule {
    public static TransactionExceptedException none() {
        return new TransactionExceptedException();
    }

    private TransactionExceptedException() {
        exception = ExpectedException.none();
    }

    public void expectInTransaction(Class<? extends Throwable> type) {
        expect(new ClassTransactionMatcher(type));
    }

    public void expectMessageInTransaction(String message) {
        expect(new MessageTransactionMatcher(message));
    }

    @Delegate
    private ExpectedException exception;

    private static class ClassTransactionMatcher extends BaseMatcher {

        private Class<?> aClass;

        public ClassTransactionMatcher(Class<?> aClass) {
            this.aClass = aClass;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof TransactionException)) {
                return false;
            }
            Throwable ex = (Throwable) item;
            while ((ex = (ex.getCause())) != null) {
                if (aClass.isAssignableFrom(ex.getClass())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            // describe nothing
        }
    }

    private static class MessageTransactionMatcher extends BaseMatcher {

        private String message;

        public MessageTransactionMatcher(String message) {
            this.message = message;
        }

        @Override
        public boolean matches(Object item) {
            if (!(item instanceof TransactionException)) {
                return false;
            }
            Throwable ex = (Throwable) item;
            while ((ex = (ex.getCause())) != null) {
                if (ex.getMessage() != null && ex.getMessage().contains(message)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            // describe nothing
        }
    }

}
