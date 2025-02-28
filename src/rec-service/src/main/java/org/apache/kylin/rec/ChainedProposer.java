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

package org.apache.kylin.rec;

import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rec.common.AccelerateInfo;

import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChainedProposer extends AbstractProposer {

    @Getter
    private final ImmutableList<AbstractProposer> proposerList;

    public ChainedProposer(AbstractContext proposeContext, ImmutableList<AbstractProposer> proposerList) {
        super(proposeContext);
        this.proposerList = proposerList;
        assert !proposerList.contains(this);
    }

    @Override
    public void execute() {
        for (AbstractProposer proposer : proposerList) {
            long start = System.currentTimeMillis();
            log.info("Enter the step of `{}`", proposer.getIdentifierName());

            proposer.execute();

            val nums = getAccelerationNumMap();
            log.info("The step of `{}` completed successfully, takes {}ms. SUCCESS {}, PENDING {}, FAILED {}.",
                    proposer.getIdentifierName(), //
                    System.currentTimeMillis() - start, //
                    nums.get(SmartMaster.AccStatusType.SUCCESS), //
                    nums.get(SmartMaster.AccStatusType.PENDING), //
                    nums.get(SmartMaster.AccStatusType.FAILED));
        }
    }

    private Map<SmartMaster.AccStatusType, Integer> getAccelerationNumMap() {
        Map<SmartMaster.AccStatusType, Integer> result = Maps.newHashMap();
        result.putIfAbsent(SmartMaster.AccStatusType.SUCCESS, 0);
        result.putIfAbsent(SmartMaster.AccStatusType.PENDING, 0);
        result.putIfAbsent(SmartMaster.AccStatusType.FAILED, 0);
        val accelerateInfoMap = proposeContext.getAccelerateInfoMap();
        for (Map.Entry<String, AccelerateInfo> entry : accelerateInfoMap.entrySet()) {
            if (entry.getValue().isPending()) {
                result.computeIfPresent(SmartMaster.AccStatusType.PENDING, (k, v) -> v + 1);
            } else if (entry.getValue().isFailed()) {
                result.computeIfPresent(SmartMaster.AccStatusType.FAILED, (k, v) -> v + 1);
            } else {
                result.computeIfPresent(SmartMaster.AccStatusType.SUCCESS, (k, v) -> v + 1);
            }
        }
        return result;
    }

    @Override
    public String getIdentifierName() {
        throw new NotImplementedException("No need to use the name of ChainProposer");
    }
}
