package org.example.cacheloader;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author Kelly Brant
 *
 */
public class Main {

    public Main() { }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("You must have two arguments: loadDB|loadRedis, filename");
        }
        Main m = new Main();
        if ("loadDB".equals(args[0])) {
            m.loadDB(args[1]);
        } else if ("loadRedis".equals(args[0])) {
            m.loadRedis(args[1]);
        } else if ("readDB".equals(args[0])) {
            m.readDB(args[1]);
        } else if ("readRedis".equals(args[0])) {
            m.readRedis(args[1]);
        }
    }

    public void loadDB(String filename) {
       System.out.println(filename);
       long time = System.currentTimeMillis();
       Map<Integer, String> dataMap= new HashMap<Integer,String>();
       for (int i = 1000; i < 1001000; i++) {
           dataMap.put(i, UUID.randomUUID().toString());
       }
       PostgresDao dao = new PostgresDao();
       dao.insert(dataMap);
       System.out.println(String.format("time %d ms", System.currentTimeMillis() - time));       
    }

    public Map<Integer, String> readDB(String filename) {
        System.out.println(filename);
        long time = System.currentTimeMillis();
        PostgresDao dao = new PostgresDao();
        Map<Integer, String> dataMap = dao.findAll();
        System.out.println("dataMap size = " + dataMap.size());
        System.out.println(String.format("item 1: %d:%s", 1000, dataMap.get(1000)));
        System.out.println(String.format("time %d ms", System.currentTimeMillis() - time));
        return dataMap;
    }

    public void loadRedis(String filename) {
        Map<Integer, String> dataMap = readDB("reading_db_to_load_redis");
        System.out.println("finished reading_db_to_load_redis");
        System.out.println(filename);
        long time = System.currentTimeMillis();
        RedisDao dao = new RedisDao();
        dao.insert(dataMap);
        System.out.println(String.format("time %d ms", System.currentTimeMillis() - time));
    }

    public void readRedis(String filename) {
        System.out.println(filename);
        long time = System.currentTimeMillis();
        RedisDao dao = new RedisDao();
        Map<Integer, String> dataMap = dao.findAll();
        System.out.println("dataMap size = " + dataMap.size());
        System.out.println(String.format("item 1: %d:%s", 1000, dataMap.get(1000)));
        System.out.println(String.format("time %d ms", System.currentTimeMillis() - time));
    }
}
