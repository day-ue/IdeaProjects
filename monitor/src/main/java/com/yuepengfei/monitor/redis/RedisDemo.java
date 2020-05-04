package com.yuepengfei.monitor.redis;

import redis.clients.jedis.Jedis;

public class RedisDemo {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.0.131", 6379);
        String offset = jedis.get("PrdKafkaWordCount.offsets");
        System.out.println(offset);
        jedis.close();
    }
}
