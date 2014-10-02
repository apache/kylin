/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.query.optrule;

import com.kylinolap.query.relnode.OLAPRel;
import com.kylinolap.query.relnode.OLAPToEnumerableConverter;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.RelTraitSet;

/**
 * @author xjiang
 */
public class OLAPToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new OLAPToEnumerableConverterRule();

    public OLAPToEnumerableConverterRule() {
        super(RelNode.class, OLAPRel.CONVENTION, EnumerableConvention.INSTANCE,
                "OLAPToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new OLAPToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }

}
