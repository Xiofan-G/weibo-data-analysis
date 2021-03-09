package org.weibo.analysis.hash;

import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashing {
    private final int VIRTUAL_COPIES = 2000;
    private TreeMap<Long, String> virtualNodes = new TreeMap<>();
    private Set<String> physicalNodes;

    public ConsistentHashing(Set<String> ipNodes) {

        physicalNodes = ipNodes;


        for (String nodeIp : physicalNodes) {

            addPhysicalNode(nodeIp);
        }
    }

    // 32-bit Fowler-Noll-Vo Hash-Algorithm
    // https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
    private Long FNVHash(String key) {
        final int p = 16777619;
        Long hash = 2166136261L;
        for (int idx = 0, num = key.length(); idx < num; ++idx) {
            hash = (hash ^ key.charAt(idx)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }


    // Add Physical Node
    private void addPhysicalNode(String nodeIp) {
        for (int idx = 0; idx < VIRTUAL_COPIES; ++idx) {
            long hash = FNVHash(nodeIp + "#" + idx);
            virtualNodes.put(hash, nodeIp);
        }
    }

    public void removePhysicalNode(String nodeIp) {
        for (int idx = 0; idx < VIRTUAL_COPIES; ++idx) {
            long hash = FNVHash(nodeIp + "#" + idx);
            virtualNodes.remove(hash);
        }
    }


    // Find object mapped nodes
    public String getObjectNode(String object) {
        long hash = FNVHash(object);
        SortedMap<Long, String> tailMap = virtualNodes.tailMap(hash); // 所有大于 hash 的节点
        Long key = tailMap.isEmpty() ? virtualNodes.firstKey() : tailMap.firstKey();
        return virtualNodes.get(key);
    }

}
