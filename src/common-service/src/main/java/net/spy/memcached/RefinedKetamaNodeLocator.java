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

package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;

/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * This is a modified version of the Ketama consistent hash strategy from
 * last.fm. This implementation may not be compatible with libketama as hashing
 * is considered separate from node location.
 *
 * The only modified method is the getSequence(). 
 * The previous 7 may be too small to reduce the chance to get all down nodes.
 *
 * Note that this implementation does not currently supported weighted nodes.
 *
 * @see <a href="http://www.last.fm/user/RJ/journal/2007/04/10/392555/">RJ's
 *      blog post</a>
 */
public final class RefinedKetamaNodeLocator extends SpyObject implements NodeLocator {

    private final HashAlgorithm hashAlg;
    private final Map<InetSocketAddress, Integer> weights;
    private final boolean isWeightedKetama;
    private final KetamaNodeLocatorConfiguration config;
    private AtomicReference<TreeMap<Long, MemcachedNode>> ketamaNodes = new AtomicReference<>();
    private AtomicReference<Collection<MemcachedNode>> allNodes = new AtomicReference<>();

    /**
     * Create a new KetamaNodeLocator using specified nodes and the specifed hash
     * algorithm.
     *
     * @param nodes The List of nodes to use in the Ketama consistent hash
     *          continuum
     * @param alg The hash algorithm to use when choosing a node in the Ketama
     *          consistent hash continuum
     */
    public RefinedKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
        this(nodes, alg, KetamaNodeKeyFormatter.Format.SPYMEMCACHED, new HashMap<>());
    }

    /**
     * Create a new KetamaNodeLocator with specific nodes, hash, node key format,
     * and weight
     *
     * @param nodes The List of nodes to use in the Ketama consistent hash
     *          continuum
     * @param alg The hash algorithm to use when choosing a node in the Ketama
     *          consistent hash continuum
     * @param nodeKeyFormat the format used to name the nodes in Ketama, either
     *          SPYMEMCACHED or LIBMEMCACHED
     * @param weights node weights for ketama, a map from InetSocketAddress to
     *          weight as Integer
     */
    public RefinedKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
            KetamaNodeKeyFormatter.Format nodeKeyFormat, Map<InetSocketAddress, Integer> weights) {
        this(nodes, alg, weights, new DefaultKetamaNodeLocatorConfiguration(new KetamaNodeKeyFormatter(nodeKeyFormat)));
    }

    /**
     * Create a new KetamaNodeLocator using specified nodes and the specifed hash
     * algorithm and configuration.
     *
     * @param nodes The List of nodes to use in the Ketama consistent hash
     *          continuum
     * @param alg The hash algorithm to use when choosing a node in the Ketama
     *          consistent hash continuum
     * @param conf
     */
    public RefinedKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
        this(nodes, alg, new HashMap<>(), conf);
    }

    /**
     * Create a new KetamaNodeLocator with specific nodes, hash, node key format,
     * and weight
     *
     * @param nodes The List of nodes to use in the Ketama consistent hash
     *          continuum
     * @param alg The hash algorithm to use when choosing a node in the Ketama
     *          consistent hash continuum
     * @param nodeWeights node weights for ketama, a map from InetSocketAddress to
     *          weight as Integer
     * @param configuration node locator configuration
     */
    public RefinedKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
            Map<InetSocketAddress, Integer> nodeWeights, KetamaNodeLocatorConfiguration configuration) {
        super();
        allNodes.set(nodes);
        hashAlg = alg;
        config = configuration;
        weights = nodeWeights;
        isWeightedKetama = !weights.isEmpty();
        setKetamaNodes(nodes);
    }

    private RefinedKetamaNodeLocator(TreeMap<Long, MemcachedNode> smn, Collection<MemcachedNode> an, HashAlgorithm alg,
            Map<InetSocketAddress, Integer> nodeWeights, KetamaNodeLocatorConfiguration conf) {
        super();
        ketamaNodes.set(smn);
        allNodes.set(an);
        hashAlg = alg;
        config = conf;
        weights = nodeWeights;
        isWeightedKetama = !weights.isEmpty();
    }

    public Collection<MemcachedNode> getAll() {
        return allNodes.get();
    }

    public MemcachedNode getPrimary(final String k) {
        MemcachedNode rv = getNodeForKey(hashAlg.hash(k));
        if (null == rv) {
            throw new IllegalArgumentException("Found no node for key" + k);
        }
        return rv;
    }

    long getMaxKey() {
        return getKetamaNodes().lastKey();
    }

    MemcachedNode getNodeForKey(long hash) {
        final MemcachedNode rv;
        if (!ketamaNodes.get().containsKey(hash)) {
            // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
            // in a lot of places, so I'm doing this myself.
            SortedMap<Long, MemcachedNode> tailMap = getKetamaNodes().tailMap(hash);
            if (tailMap.isEmpty()) {
                hash = getKetamaNodes().firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }
        rv = getKetamaNodes().get(hash);
        return rv;
    }

    /**
     * the previous 7 may be too small to reduce the chance to get all down nodes
     * @param k
     * @return
     */
    public Iterator<MemcachedNode> getSequence(String k) {
        // Seven searches gives us a 1 in 2^maxTry chance of hitting the
        // same dead node all of the time.
        int maxTry = config.getNodeRepetitions() + 1;
        if (maxTry < 20) {
            maxTry = 20;
        }
        return new KetamaIterator(k, maxTry, getKetamaNodes(), hashAlg);
    }

    public NodeLocator getReadonlyCopy() {
        TreeMap<Long, MemcachedNode> smn = new TreeMap<>(getKetamaNodes());
        Collection<MemcachedNode> an = new ArrayList<>(allNodes.get().size());

        // Rewrite the values a copy of the map.
        for (Map.Entry<Long, MemcachedNode> me : smn.entrySet()) {
            smn.put(me.getKey(), new MemcachedNodeROImpl(me.getValue()));
        }

        // Copy the allNodes collection.
        for (MemcachedNode n : allNodes.get()) {
            an.add(new MemcachedNodeROImpl(n));
        }

        return new RefinedKetamaNodeLocator(smn, an, hashAlg, weights, config);
    }

    @Override
    public void updateLocator(List<MemcachedNode> nodes) {
        allNodes.set(nodes);
        setKetamaNodes(nodes);
    }

    /**
     * @return the ketamaNodes
     */
    protected TreeMap<Long, MemcachedNode> getKetamaNodes() {
        return ketamaNodes.get();
    }

    /**
     * Setup the KetamaNodeLocator with the list of nodes it should use.
     *
     * @param nodes a List of MemcachedNodes for this KetamaNodeLocator to use in
     *          its continuum
     */
    @SuppressWarnings({ "squid:S3776" })
    protected void setKetamaNodes(List<MemcachedNode> nodes) {
        TreeMap<Long, MemcachedNode> newNodeMap = new TreeMap<>();
        int numReps = config.getNodeRepetitions();
        int nodeCount = nodes.size();
        int totalWeight = 0;

        if (isWeightedKetama) {
            for (MemcachedNode node : nodes) {
                totalWeight += weights.get(node.getSocketAddress());
            }
        }

        for (MemcachedNode node : nodes) {
            if (isWeightedKetama) {

                int thisWeight = weights.get(node.getSocketAddress());
                float percent = (totalWeight == 0 ? 0f : (float) thisWeight / (float) totalWeight);
                int pointerPerServer = (int) ((Math
                        .floor((float) (percent * config.getNodeRepetitions() / 4 * nodeCount + 0.0000000001))) * 4);
                for (int i = 0; i < pointerPerServer / 4; i++) {
                    for (long position : ketamaNodePositionsAtIteration(node, i)) {
                        newNodeMap.put(position, node);
                        getLogger().debug("Adding node %s with weight %s in position %d", node, thisWeight, position);
                    }
                }
            } else {
                // Ketama does some special work with md5 where it reuses chunks.
                // Check to be backwards compatible, the hash algorithm does not
                // matter for Ketama, just the placement should always be done using
                // MD5
                if (hashAlg == DefaultHashAlgorithm.KETAMA_HASH) {
                    for (int i = 0; i < numReps / 4; i++) {
                        for (long position : ketamaNodePositionsAtIteration(node, i)) {
                            newNodeMap.put(position, node);
                            getLogger().debug("Adding node %s in position %d", node, position);
                        }
                    }
                } else {
                    for (int i = 0; i < numReps; i++) {
                        newNodeMap.put(hashAlg.hash(config.getKeyForNode(node, i)), node);
                    }
                }
            }
        }
        assert newNodeMap.size() == numReps * nodes.size();
        ketamaNodes.set(newNodeMap);
    }

    private List<Long> ketamaNodePositionsAtIteration(MemcachedNode node, int iteration) {
        List<Long> positions = new ArrayList<>();
        byte[] digest = DefaultHashAlgorithm.computeMd5(config.getKeyForNode(node, iteration));
        for (int h = 0; h < 4; h++) {
            long k = ((long) (digest[3 + h * 4] & 0xFF) << 24) | ((long) (digest[2 + h * 4] & 0xFF) << 16);
            k |= ((long) (digest[1 + h * 4] & 0xFF) << 8) | (digest[h * 4] & 0xFF);
            positions.add(k);
        }
        return positions;
    }
}
