package com.grig.spingbootredis.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Client;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.ShardedJedisSentinelPool;

import java.util.Map;
import java.util.Set;

/**
 * Redis工具类
 */
@Component
public class RedisSentinelShardedUtil  implements ApplicationContextAware {

    /**
     * Jedis对象池 所有Jedis对象均通过该池租赁获取
     */
    private static ShardedJedisSentinelPool shardedPool;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        shardedPool = applicationContext.getBean("shardedJedisSentinelPool", ShardedJedisSentinelPool.class);
    }

    /**
     * 获取到Jedis
     */
    public static ShardedJedis getJedis() {
        return shardedPool.getResource();
    }





    /**
     * 释放Jedis
     *
     * @param jedis
     */
    public static void releaseJedis(ShardedJedis jedis) {
        if (jedis != null) {
            //返回到池中
            jedis.close();
        }

    }

    /**
     * 缓存放入
     *
     * @return
     */
    public static String set(String key, String value) {
        ShardedJedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            result = jedis.set(key, value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * set添加值
     *
     * @return
     */
    public static Long sadd(String value) {
        ShardedJedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            result = jedis.sadd("keyset", value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * set移除值
     *
     * @return
     */
    public static Long srem(String value) {
        ShardedJedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            result = jedis.srem("keyset", value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * set所有成员
     *
     * @return
     */
    public static Set<String> smembers() {
        ShardedJedis jedis = null;
        Set<String> result = null;
        try {
            jedis = getJedis();
            result = jedis.smembers("keyset");
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 有过期时间缓存放入
     *
     * @return
     */
    public static String set(String key, String value, int exTime) {
        ShardedJedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            result = jedis.setex(key, exTime, value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 刪除緩存
     *
     * @return
     */
    public static Long del(String key) {
        ShardedJedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            result = jedis.del(key);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 获取緩存
     *
     * @return
     */
    public static String get(String key) {
        ShardedJedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            result = jedis.get(key);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 设置緩存过期时间
     *
     * @return
     */
    public static Long expire(String key, int exTime) {
        ShardedJedis jedis = null;
        Long result = null;
        try {
            jedis = getJedis();
            result = jedis.expire(key, exTime);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 设置緩存
     *
     * @return
     */
    public static String set(String key, Map<String, String> value) {
        ShardedJedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            result = jedis.hmset(key, value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 获取緩存
     *
     * @return
     */
    public static Map<String, String> getMap(String key) {
        ShardedJedis jedis = null;
        Map<String, String> result = null;
        try {
            jedis = getJedis();
            result = jedis.hgetAll(key);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 获取緩存
     *
     * @return
     */
    public static String getDetail(String key) {
        ShardedJedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            Client client =jedis.getShard(key).getClient();
            result ="value:"+jedis.get(key)+ " in server:" + client.getHost() + " and port is:" + client.getPort();
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }
}
