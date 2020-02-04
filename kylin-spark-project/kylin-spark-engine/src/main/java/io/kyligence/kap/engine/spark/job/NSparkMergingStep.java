/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Arrays;
import java.util.Set;

import io.kyligence.kap.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSparkMergingStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingStep.class);

    // called by reflection
    public NSparkMergingStep(){}

    public NSparkMergingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCubeByUuid(cubeId);
        return MetaDumpUtil.collectCubeMetadata(cubeInstance);
    }

    public static class Mockup {
        public static void main(String[] args) {
            logger.info(NSparkMergingStep.Mockup.class + ".main() invoked, args: " + Arrays.toString(args));
        }
    }

    /*@Override
    public void mergerMetadata(MetadataMerger merger) {
        merger.merge(this);
    }*/

    @Override
    public boolean needMergeMetadata() {
        return true;
    }

}
