package org.example.cacheloader;

import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Jedis;

/**
 * 
 * @author Kelly Brant
 *
 */
public class RedisDao {

    public Map<Integer, String> findAll() {
        Map<Integer, String> dataMap = new HashMap<Integer, String>();
        Jedis jedis = new Jedis("localhost");
        for (int key = 1000; key < 1001000; key++) {
            dataMap.put(new Integer(key), jedis.get(Integer.toString(key)));
        }
        jedis.close();
        return dataMap;
    }

    public void insert(Map<Integer, String> dataMap) {
        Jedis jedis = new Jedis("localhost");
        for (Integer key : dataMap.keySet()) {
            jedis.set(key.toString(), dataMap.get(key));
        }
        jedis.close();
    }

}
