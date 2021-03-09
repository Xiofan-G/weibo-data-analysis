package org.weibo.analysis.hash;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HashPartition implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        Map<String, Integer> ipMap = this.buildNodeIp(numPartitions);
        //put ip in ConsistenHashing
        ConsistentHashing hashService = new ConsistentHashing(ipMap.keySet());
        //Get the corresponding ip by id
        String targetIp = hashService.getObjectNode(key);

        return ipMap.get(targetIp);
    }

    //Simulate ip
    private Map<String, Integer> buildNodeIp(int numPartitions) {
        Map<String, Integer> ipMap = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < numPartitions; i++) {
            String ip = String.format(
                    "%d.%d.%d.%d",
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1,
                    random.nextInt(255) % (255) + 1);
            while (ipMap.containsKey(ip)) {
                ip = String.format(
                        "%d.%d.%d.%d",
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1,
                        random.nextInt(255) % (255) + 1);
            }

            ipMap.put(ip, i);
        }
        return ipMap;
    }
}