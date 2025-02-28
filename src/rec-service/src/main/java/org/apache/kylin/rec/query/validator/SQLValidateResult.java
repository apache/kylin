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

package org.apache.kylin.rec.query.validator;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rec.query.SQLResult;
import org.apache.kylin.rec.query.advisor.SQLAdvice;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SQLValidateResult implements Serializable {

    private static final long serialVersionUID = -1L;
    private boolean capable;
    private SQLResult result;
    private Set<SQLAdvice> sqlAdvices = Sets.newHashSet();

    static SQLValidateResult successStats(SQLResult sqlResult) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(true);
        stats.setResult(sqlResult);
        return stats;
    }

    static SQLValidateResult failedStats(List<SQLAdvice> sqlAdvices, SQLResult sqlResult) {
        SQLValidateResult stats = new SQLValidateResult();
        stats.setCapable(false);
        stats.setSqlAdvices(Sets.newHashSet(sqlAdvices));
        stats.setResult(sqlResult);
        return stats;
    }

    @Override
    public String toString() {
        StringBuilder message = new StringBuilder();
        getSqlAdvices().forEach(sqlAdvice -> {
            message.append("\n");
            message.append(String.format(Locale.ROOT, "reason: %s", sqlAdvice.getIncapableReason().replace(",", "，")));
            message.append(";");
            message.append(String.format(Locale.ROOT, "suggest: %s", sqlAdvice.getSuggestion().replace(",", "，")));
        });

        return String.format(Locale.ROOT, "capable:%s %s", isCapable(), message.toString());
    }
}
