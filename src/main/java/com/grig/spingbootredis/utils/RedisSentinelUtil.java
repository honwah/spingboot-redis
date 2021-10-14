package com.grig.spingbootredis.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelSlavePool;


import java.util.Map;

/**
 * Redis工具类
 */
@Component
public class RedisSentinelUtil implements ApplicationContextAware {

    /**
     * Jedis对象池 所有Jedis对象均通过该池租赁获取
     */
    private static JedisSentinelSlavePool sentinelPool;

    /**
     * 获取到Jedis
     */
    public static Jedis getJedis() {
        return sentinelPool.getResource();
    }

    /**
     * 获取到Jedis
     */
    public static Jedis getSlaveJedis() {
        return sentinelPool.getSlaveResource();
    }

    /**
     * 释放Jedis
     *
     * @param jedis
     */
    public static void releaseJedis(Jedis jedis) {
        if (jedis != null) {
            //返回到池中
            sentinelPool.returnResource(jedis);
        }

    }

    /**
     * 缓存放入
     *
     * @return
     */
    public static String set(String key, String value) {
        Jedis jedis = null;
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
     * 有过期时间缓存放入
     *
     * @return
     */
    public static String set(String key, String value, int exTime) {
        Jedis jedis = null;
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
        Jedis jedis = null;
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
        Jedis jedis = null;
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
        Jedis jedis = null;
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
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getJedis();
            jedis.select(15);
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
        Jedis jedis = null;
        Map<String, String> result = null;
        try {
            jedis = getJedis();
            jedis.select(15);
            result = jedis.hgetAll(key);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }
    /**
     * 程序端读写分离测试，理论上应该失败
     *
     * @return
     */
    public static String rwSplittingWrite(String key, String value) {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getSlaveJedis();
            result = jedis.set(key, value);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }

    /**
     * 程序端读写分离测试，理论上应该成功
     *
     * @return
     */
    public static String rwSplittingRead(String key) {
        Jedis jedis = null;
        String result = null;
        try {
            jedis = getSlaveJedis();
            result = jedis.get(key);
        } finally {
            releaseJedis(jedis);
        }
        return result;
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        sentinelPool = applicationContext.getBean("jedisSentinelPool", JedisSentinelSlavePool.class);
    }


}
