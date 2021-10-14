package com.grig.spingbootredis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@PropertySource("classpath:redis/redis.properties")
public class RedisProperties {
    @Value("${redis.host}")
    private  String host;

    @Value("${redis.port}")
    private  int port;

    @Value("${redis.password}")
    private  String passwd;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPasswd() {
        return passwd;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public List<String> getSentinelNodes() {
        return sentinelNodes;
    }

    public String getSentinelMaster() {
        return sentinelMaster;
    }

    public List<String> getShardedNodes() {
        return shardedNodes;
    }

    public Map<String,Set<String>>  getShardedSentinelNodes() {
        return shardedSentinelNodes;
    }

    @Value("${redis.timeout}")
    private  int timeout;

    @Value("${redis.pool.maxIdle}")
    private  int maxIdle;

    @Value("${redis.pool.maxWaitMillis}")
    private  int maxWaitMillis;

    @Value("${redis.pool.maxTotal}")
    private  int maxTotal;

    @Value("#{'${redis.sentinel.nodes}'.split(',')}")
    private List<String> sentinelNodes;

    @Value("${redis.sentinel.master}")
    private  String sentinelMaster;

    @Value("#{'${redis.sharded.nodes}'.split(',')}")
    private List<String> shardedNodes;

    @Value("#{${redis.sharded.sentinel.nodes}}")
    private Map<String,Set<String>> shardedSentinelNodes;

}
