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

package org.apache.kylin.tool.garbage;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.validation.constraints.NotNull;

/**
 * The priority of the task to be executed is:
 *   ROUTINE < CLI < API
 */
public abstract class AbstractComparableCleanTask implements Runnable, Comparable<AbstractComparableCleanTask> {
    private final CompletableFuture<Void> watcher = new CompletableFuture<>();

    public String getName() {
        return getClass().getName();
    }

    public String getBrief() {
        return String.format(Locale.ROOT, "Task-%s: {%s}", getName(), details());
    }

    protected String details() {
        return String.format(Locale.ROOT, "tag: %s, class: %s", getCleanerTag(), getClass().getName());
    }

    public CompletableFuture<Void> getWatcher() {
        return watcher;
    }

    public abstract StorageCleaner.CleanerTag getCleanerTag();

    @Override
    public void run() {
        try {
            doRun();
            watcher.complete(null);
        } catch (Throwable t) {
            watcher.completeExceptionally(t);
        }
    }

    protected void doRun() {
    }

    /**
     * Note: this class has a natural ordering that is inconsistent with equals.
     */
    @Override
    public int compareTo(@NotNull AbstractComparableCleanTask t) {
        if (this == t || getCleanerTag() == t.getCleanerTag()) {
            return 0;
        } else if (getCleanerTag().ordinal() > t.getCleanerTag().ordinal()) {
            return -1;
        } else {
            return 1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractComparableCleanTask that = (AbstractComparableCleanTask) o;
        return Objects.equals(watcher, that.watcher) && Objects.equals(getCleanerTag(), that.getCleanerTag());
    }

    @Override
    public int hashCode() {
        return Objects.hash(watcher, getCleanerTag());
    }
}
