package com.grig.spingbootredis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import redis.clients.jedis.*;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Configuration
@EnableAutoConfiguration
public class RedisConfig {

    @Resource
    private  RedisProperties redisProperties;


    /**
     * 初始化连接池配置对象
     * @return JedisPoolConfig
     */
    @Bean(value = "jedisPoolConfig")
    public JedisPoolConfig getRedisPoolConfig() {
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(redisProperties.getMaxTotal());
        config.setMaxIdle(redisProperties.getMaxIdle());
        config.setMaxWaitMillis(redisProperties.getMaxWaitMillis());
        return config;
    }

    @Bean(value = "jedisSentinelPool")
    public JedisSentinelSlavePool getJedisPool(@Qualifier(value = "jedisPoolConfig") JedisPoolConfig jedisPoolConfig) {
        Set<String> nodeSet = new HashSet<>();
        redisProperties.getSentinelNodes().forEach(x->{
            nodeSet.add(x);
        });
        JedisSentinelSlavePool jedisPool = new JedisSentinelSlavePool(redisProperties.getSentinelMaster() ,nodeSet ,jedisPoolConfig ,redisProperties.getTimeout(), redisProperties.getPasswd());
        return jedisPool;
    }
//被ShardedJedisSentinelPool代替
//    @Bean(value = "shardedJedisPool")
//    public ShardedJedisPool getShardedJedisPool(@Qualifier(value = "jedisPoolConfig") JedisPoolConfig jedisPoolConfig) {
//        List<JedisShardInfo> nodeList = new LinkedList<JedisShardInfo>();
//        redisProperties.getShardedNodes().forEach(x->{
//            JedisShardInfo jedisShardInfo = new JedisShardInfo(HostAndPort.parseString(x));
//            jedisShardInfo.setPassword(redisProperties.getPasswd());
//            jedisShardInfo.setConnectionTimeout(redisProperties.getTimeout());
//            nodeList.add(jedisShardInfo);
//        });
//
//        ShardedJedisPool jedisPool = new ShardedJedisPool(jedisPoolConfig ,nodeList);
//        return jedisPool;
//    }

    @Bean(value = "shardedJedisSentinelPool")
    public ShardedJedisSentinelPool getShardedJedisSentinelPool(@Qualifier(value = "jedisPoolConfig") JedisPoolConfig jedisPoolConfig) {
        ShardedJedisSentinelPool jedisPool = new ShardedJedisSentinelPool(redisProperties.getShardedSentinelNodes(),jedisPoolConfig,redisProperties.getPasswd(),redisProperties.getTimeout());
        return jedisPool;
    }
}
