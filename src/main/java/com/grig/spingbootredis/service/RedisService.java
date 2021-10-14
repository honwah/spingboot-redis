package com.grig.spingbootredis.service;


import com.grig.spingbootredis.utils.RedisSentinelShardedUtil;
import com.grig.spingbootredis.utils.RedisSentinelUtil;
//import com.grig.spingbootredis.utils.RedisShardedUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

@Service
public class RedisService {



    /**
     * 缓存放入
     *
     * @return
     */
    public  String set1(String key, String value) {
       return RedisSentinelUtil.set(key, value);
    }

    /**
     * 有过期时间缓存放入
     *
     * @return
     */
    public  String set2(String key, String value, int exTime) {
        return RedisSentinelUtil.set(key, value,exTime);
    }

    /**
     * 刪除緩存
     *
     * @return
     */
    public  Long del(String key) {
        return RedisSentinelUtil.del(key);
    }

    /**
     * 获取緩存
     *
     * @return
     */
    public  String get1(String key) {
        return RedisSentinelUtil.get(key);
    }

    /**
     * 设置緩存过期时间
     *
     * @return
     */
    public  Long expire(String key, int exTime) {
        return RedisSentinelUtil.expire(key, exTime);
    }

    /**
     * 设置緩存
     *
     * @return
     */
    public  String set3(String key, Map<String, String> value) {
        return RedisSentinelUtil.set(key, value);
    }

    /**
     * 获取緩存
     *
     * @return
     */
    public  Map<String, String> get2(String key) {
        return RedisSentinelUtil.getMap(key);
    }

    /**
     * 程序端读写分离测试
     *
     * @return
     */
    public  String rwSplittingWrite(String key, String value) {
        return RedisSentinelUtil.rwSplittingWrite(key,value);
    }

    /**
     * 程序端读写分离测试
     *
     * @return
     */
    public  String rwSplittingRead(String key) {
        return RedisSentinelUtil.rwSplittingRead(key);
    }

//    public  String shardedJedisDemoWrite() {
//
//        RedisShardedUtil.set("数据结构", "数据结构",120);
//        RedisShardedUtil.set("算法", "算法",120);
//        RedisShardedUtil.set("操作系统", "操作系统",120);
//        RedisShardedUtil.set("计算机网络", "计算机网络",120);
//
//        return "OK";
//    }
//
//    public  String shardedJedisDemoRead() {
//
//
//        StringBuffer sb = new StringBuffer();
//        //打印key在哪个server中
//
//        try {
//            sb.append(RedisShardedUtil.getDetail("数据结构")+"<br />");
//        } catch (Exception e) {
//            sb.append(e+"<br />");
//        }
//
//        try {
//            sb.append(RedisShardedUtil.getDetail("算法")+"<br />");
//        } catch (Exception e) {
//            sb.append(e+"<br />");
//        }
//        try {
//            sb.append(RedisShardedUtil.getDetail("操作系统")+"<br />");
//        } catch (Exception e) {
//            sb.append(e+"<br />");
//        }
//        try {
//            sb.append(RedisShardedUtil.getDetail("计算机网络")+"<br />");
//        } catch (Exception e) {
//            sb.append(e+"<br />");
//        }
//        return sb.toString();
//    }

    public  String shardedSentinelJedisDemoWrite(String key ,String value) {

        RedisSentinelShardedUtil.set(key, value,360);
        RedisSentinelShardedUtil.sadd(key);
        return "OK";
    }

    public  String shardedSentinelJedisDemoDel(String key) {

        RedisSentinelShardedUtil.del(key);
        RedisSentinelShardedUtil.srem(key);
        return "OK";
    }

    public  String shardedSentinelJedisDemoRead(String key) {


        StringBuffer sb = new StringBuffer();
        //打印key在哪个server中

        try {
            sb.append(RedisSentinelShardedUtil.getDetail("计算机网络")+"<br />");
        } catch (Exception e) {
            sb.append(e+"<br />");
        }
        return sb.toString();
    }

    public  String shardedSentinelJedisDemoReadAll() {
 //读取所有key

        StringBuffer sb = new StringBuffer();
        //打印key在哪个server中
       Set<String>  keys = RedisSentinelShardedUtil.smembers();
        for (String str: keys ) {
            try {
                sb.append(RedisSentinelShardedUtil.getDetail(str)+"<br />");
            } catch (Exception e) {
                sb.append(e+"<br />");
            }
        }
        return sb.toString();
    }

}
