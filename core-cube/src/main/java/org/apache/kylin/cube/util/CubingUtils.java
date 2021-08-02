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

package org.apache.kylin.cube.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hasher;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

/**
 */
public class CubingUtils {

    private static Logger logger = LoggerFactory.getLogger(CubingUtils.class);

    public static Map<Long, HLLCounter> sampling(CubeDesc cubeDesc, IJoinedFlatTableDesc flatDescIn,
            Iterable<List<String>> streams) {
        final CubeJoinedFlatTableEnrich flatDesc = new CubeJoinedFlatTableEnrich(flatDescIn, cubeDesc);
        final int rowkeyLength = cubeDesc.getRowkey().getRowKeyColumns().length;
        final Set<Long> allCuboidIds = cubeDesc.getInitialCuboidScheduler().getAllCuboidIds();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Map<Long, Integer[]> allCuboidsBitSet = Maps.newHashMap();

        final Map<Long, HLLCounter> result = Maps.newHashMapWithExpectedSize(allCuboidIds.size());
        for (Long cuboidId : allCuboidIds) {
            result.put(cuboidId, new HLLCounter(cubeDesc.getConfig().getCubeStatsHLLPrecision()));
            Integer[] cuboidBitSet = new Integer[Long.bitCount(cuboidId)];

            long mask = Long.highestOneBit(baseCuboidId);
            int position = 0;
            for (int i = 0; i < rowkeyLength; i++) {
                if ((mask & cuboidId) > 0) {
                    cuboidBitSet[position] = i;
                    position++;
                }
                mask = mask >> 1;
            }
            allCuboidsBitSet.put(cuboidId, cuboidBitSet);
        }

        HashFunction hf = Hashing.murmur3_32();
        byte[][] row_hashcodes = new byte[rowkeyLength][];
        for (List<String> row : streams) {
            //generate hash for each row key column
            for (int i = 0; i < rowkeyLength; i++) {
                Hasher hc = hf.newHasher();
                final String cell = row.get(flatDesc.getRowKeyColumnIndexes()[i]);
                if (cell != null) {
                    row_hashcodes[i] = hc.putUnencodedChars(cell).hash().asBytes();
                } else {
                    row_hashcodes[i] = hc.putInt(0).hash().asBytes();
                }
            }

            for (Map.Entry<Long, HLLCounter> longHyperLogLogPlusCounterNewEntry : result.entrySet()) {
                Long cuboidId = longHyperLogLogPlusCounterNewEntry.getKey();
                HLLCounter counter = longHyperLogLogPlusCounterNewEntry.getValue();
                Hasher hc = hf.newHasher();
                final Integer[] cuboidBitSet = allCuboidsBitSet.get(cuboidId);
                for (int position = 0; position < cuboidBitSet.length; position++) {
                    hc.putBytes(row_hashcodes[cuboidBitSet[position]]);
                }
                counter.add(hc.hash().asBytes());
            }
        }
        return result;
    }

//    public static Map<TblColRef, Dictionary<String>> buildDictionary(final CubeInstance cubeInstance,
//            Iterable<List<String>> recordList) throws IOException {
//        final List<TblColRef> columnsNeedToBuildDictionary = cubeInstance.getDescriptor()
//                .listDimensionColumnsExcludingDerived(true);
//        final HashMap<Integer, TblColRef> tblColRefMap = Maps.newHashMap();
//        int index = 0;
//        for (TblColRef column : columnsNeedToBuildDictionary) {
//            tblColRefMap.put(index++, column);
//        }
//
//        HashMap<TblColRef, Dictionary<String>> result = Maps.newHashMap();
//
//        HashMultimap<TblColRef, String> valueMap = HashMultimap.create();
//        for (List<String> row : recordList) {
//            for (int i = 0; i < row.size(); i++) {
//                String cell = row.get(i);
//                if (tblColRefMap.containsKey(i)) {
//                    valueMap.put(tblColRefMap.get(i), cell);
//                }
//            }
//        }
//        for (TblColRef tblColRef : valueMap.keySet()) {
//            Set<String> values = valueMap.get(tblColRef);
//            Dictionary<String> dict = DictionaryGenerator.buildDictionary(tblColRef.getType(),
//                    new IterableDictionaryValueEnumerator(values));
//            result.put(tblColRef, dict);
//        }
//        return result;
//    }

//    @SuppressWarnings("unchecked")
//    public static Map<TblColRef, Dictionary<String>> writeDictionary(CubeSegment cubeSegment,
//            Map<TblColRef, Dictionary<String>> dictionaryMap, long startOffset, long endOffset) {
//        Map<TblColRef, Dictionary<String>> realDictMap = Maps.newHashMap();
//
//        for (Map.Entry<TblColRef, Dictionary<String>> entry : dictionaryMap.entrySet()) {
//            final TblColRef tblColRef = entry.getKey();
//            final Dictionary<String> dictionary = entry.getValue();
//            IReadableTable.TableSignature signature = new IReadableTable.TableSignature();
//            signature.setLastModifiedTime(System.currentTimeMillis());
//            signature.setPath(String.format(Locale.ROOT, "streaming_%s_%s", startOffset, endOffset));
//            signature.setSize(endOffset - startOffset);
//            DictionaryInfo dictInfo = new DictionaryInfo(tblColRef.getColumnDesc(), tblColRef.getDatatype(), signature);
//            logger.info("writing dictionary for TblColRef:" + tblColRef.toString());
//            DictionaryManager dictionaryManager = DictionaryManager.getInstance(cubeSegment.getCubeDesc().getConfig());
//            try {
//                DictionaryInfo realDict = dictionaryManager.trySaveNewDict(dictionary, dictInfo);
//                cubeSegment.putDictResPath(tblColRef, realDict.getResourcePath());
//                realDictMap.put(tblColRef, (Dictionary<String>) realDict.getDictionaryObject());
//            } catch (IOException e) {
//                throw new RuntimeException("error save dictionary for column:" + tblColRef, e);
//            }
//        }
//
//        return realDictMap;
//    }

}
