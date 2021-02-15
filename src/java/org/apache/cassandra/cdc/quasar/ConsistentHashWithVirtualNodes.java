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
package org.apache.cassandra.cdc.quasar;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consistent hash with virtual nodes,
 * see https://programmer.help/blogs/consistency-hash-algorithm-principle-and-java-implementation.html
 */
public class ConsistentHashWithVirtualNodes
{
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashWithVirtualNodes.class);

    final int numberOfVirtualNodes;
    int numberOfNodes;

    //Virtual node, key represents the hash value of virtual node, value represents the name of virtual node
    SortedMap<Integer, String> virtualNodes;
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    public ConsistentHashWithVirtualNodes(int numberOfNode) {
        this(16, numberOfNode);
    }

    public ConsistentHashWithVirtualNodes(int numberOfVirtualNodes, int numberOfNode) {
        this.numberOfVirtualNodes = numberOfVirtualNodes;
        this.numberOfNodes = numberOfNodes;
        this.virtualNodes = computeVirtualNodes(numberOfVirtualNodes, numberOfNodes);
    }

    public void updateNumberOfNode(int numberOfNode) {
        w.lock();
        try {
            this.numberOfNodes = numberOfNode;
            this.virtualNodes = computeVirtualNodes(this.numberOfVirtualNodes, this.numberOfNodes);
            logger.debug("virtualNodes={}", virtualNodes);
        }
        finally
        {
            w.unlock();
        }

    }

    TreeMap<Integer, String> computeVirtualNodes(int numberOfVirtualNode, int numberOfNodes) {
        TreeMap<Integer, String> virtualNodes = new TreeMap<>();
        //Adding virtual nodes makes traversing LinkedList more efficient using foreach loops
        for (int n = 0; n < numberOfNodes; n++){
            for(int i=0; i < numberOfVirtualNode; i++){
                String virtualNodeName = n + ":" + i;
                int hash = Murmur3HashFunction.hash(virtualNodeName);
                logger.debug("virtualNode={} hash={}", virtualNodeName, hash);
                virtualNodes.put(hash, virtualNodeName);
            }
        }
        return virtualNodes;
    }

    public int getOrdinal(int hash) {
        r.lock();
        try
        {
            // Get all Map s that are larger than the Hash value
            SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);
            String virtualNode;
            if (subMap.isEmpty())
            {
                //If there is no one larger than the hash value of the key, start with the first node
                Integer i = virtualNodes.firstKey();
                //Return to the corresponding server
                virtualNode = virtualNodes.get(i);
            }
            else
            {
                //The first Key is the nearest node clockwise past the node.
                Integer i = subMap.firstKey();
                //Return to the corresponding server
                virtualNode = subMap.get(i);
            }
            //The virtual Node virtual node name needs to be intercepted
            if (virtualNode != null && virtualNode.length() > 0)
            {
                logger.debug("virtualNode={} for hash={}", virtualNode, hash);
                return Integer.parseInt(virtualNode.substring(0, virtualNode.indexOf(":")));
            }
            throw new IllegalStateException("virtualNode not found for hash=" + hash);
        }
        finally
        {
            r.unlock();
        }
    }
}
