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

package org.apache.kylin.engine.spark.metadata.cube.cuboid;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;
import org.apache.kylin.engine.spark.metadata.cube.model.IndexEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;

import java.util.Comparator;
import java.util.Optional;

public class NCuboidLayoutChooser {

    public static LayoutEntity selectLayoutForBuild(NDataSegment segment, IndexEntity entity) {
        Optional<LayoutEntity> candidate = segment.getIndexPlan().getAllIndexes().stream() //
                .filter(index -> index.fullyDerive(entity)) //
                .flatMap(index -> index.getLayouts().stream()) //
                .filter(layout -> (segment.getLayout(layout.getId()) != null)) //
                .min(Comparator.comparingLong(o -> getRows(o, segment))); //
        return candidate.orElse(null);
    }

    private static long getRows(LayoutEntity o1, NDataSegment segment) {
        return segment.getLayout(o1.getId()).getRows();
    }
}