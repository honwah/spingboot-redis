package redis.clients.jedis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Hashing;
import redis.clients.jedis.util.Pool;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * jedis分片集群，支持集群自动切换master
 */
public class ShardedJedisSentinelPool extends Pool<ShardedJedis> {

    protected final Logger log;
    private final Object initPoolLock;
    protected GenericObjectPoolConfig poolConfig;
    protected int timeout;
    protected String password;
    protected int sentinelConnectionTimeout;
    protected int sentinelSoTimeout;
    protected String sentinelClientName;
    protected Set<MasterListener> masterListeners;
    private final int sentinelRetry;
    private volatile List<HostAndPort> currentHostMasters=Collections.synchronizedList(new ArrayList<HostAndPort>());

    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels) {
        this(sentinels, new GenericObjectPoolConfig());
    }


    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels, GenericObjectPoolConfig poolConfig) {
        this(sentinels, poolConfig, null);
    }


    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels,
                                    GenericObjectPoolConfig poolConfig, String password) {
        this(sentinels, poolConfig, password, Protocol.DEFAULT_TIMEOUT);
    }


    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels,
                                    GenericObjectPoolConfig poolConfig, String password, int timeout) {
        this(sentinels, poolConfig, password, timeout, null);
    }

    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels,
                                    GenericObjectPoolConfig poolConfig, String password, int timeout,
                                    String sentinelClientName) {
        this(sentinels, poolConfig, password, timeout,  Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT,
                null);
    }

    public ShardedJedisSentinelPool(Map<String, Set<String>> sentinels,
                                    GenericObjectPoolConfig poolConfig, String password, int timeout, int sentinelConnectionTimeout, int sentinelSoTimeout,
                                    String sentinelClientName) {
        this.initPoolLock = new Object();
        this.poolConfig = poolConfig;
        this.sentinelRetry = 0;
        this.timeout = timeout;
        this.password = password;
        this.sentinelConnectionTimeout = sentinelConnectionTimeout;
        this.sentinelSoTimeout = sentinelSoTimeout;
        this.sentinelClientName = sentinelClientName;
        this.masterListeners = new HashSet();
        this.log = Logger.getLogger(getClass().getName());
        List<HostAndPort> masterList = initSentinels(sentinels);
        initPool(masterList);
    }

    public void destroy() {
        for (MasterListener m : this.masterListeners) {
            m.shutdown();
        }

        super.destroy();
    }

    public List<HostAndPort> getCurrentHostMaster() {
        return this.currentHostMasters;
    }

    private void initPool(List<HostAndPort> masters) {
        synchronized (this.initPoolLock) {
            if (!currentHostMasters.equals(masters)) {
                String masterPlain = masters.stream().map(HostAndPort::toString).collect(Collectors.joining(","));
                log.info("Created ShardedJedisPool to master at [" + masterPlain + "]");

                List<JedisShardInfo> shardMasters = new ArrayList<JedisShardInfo>();
                masters.forEach(x -> {
                    JedisShardInfo jedisShardInfo = new JedisShardInfo(x.getHost(), x.getPort(), this.timeout);
                    jedisShardInfo.setPassword(this.password);
                    shardMasters.add(jedisShardInfo);
                });
                initPool(this.poolConfig, new ShardedJedisFactory(shardMasters, Hashing.MURMUR_HASH, null));
                currentHostMasters = masters;
            }
        }
    }


    private List<HostAndPort> initSentinels(Map<String, Set<String>> sentinels) {
        List<HostAndPort> shardMasters = new ArrayList<HostAndPort>();
        log.info("Trying to find all master from available Sentinels...");
        sentinels.forEach((key, value) -> {
            String masterName = key;
            HostAndPort master = null;
            boolean sentinelAvailable = false;
            Iterator var5 = value.iterator();
            String sentinel;
            HostAndPort hap;
            while (var5.hasNext()) {
                sentinel = (String) var5.next();
                hap = HostAndPort.parseString(sentinel);
                log.fine("Connecting to Sentinel " + hap);
                Jedis jedis = null;
                try {
                    jedis = new Jedis(hap.getHost(), hap.getPort(), this.sentinelConnectionTimeout, this.sentinelSoTimeout);

                    if (this.sentinelClientName != null) {
                        jedis.clientSetname(this.sentinelClientName);
                    }
                    List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
                    sentinelAvailable = true;
                    if (masterAddr != null && masterAddr.size() == 2) {
                        master = toHostAndPort(masterAddr);
                        log.fine("Found Redis master at " + master);
                        shardMasters.add(master);
                        jedis.disconnect();
                        break;
                    }
                } catch (JedisException e) {
                    log.warning("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                } finally {
                    if (jedis != null) {
                        jedis.close();
                    }

                }

                if (master == null) {
                    if (sentinelAvailable) {
                        throw new JedisException("Can connect to sentinel, but " + masterName + " seems to be not monitored...");
                    } else {
                        throw new JedisConnectionException("All sentinels down, cannot determine where is " + masterName + " master is running...");
                    }
                } else {
                    log.info("Redis master running at " + master + ", starting Sentinel listeners...");
                }
            }
        });

        // All shards master must been accessed.
        if (sentinels.size() != 0 && sentinels.size() == shardMasters.size()) {
            log.info("Starting Sentinel listeners...");
            List<String> masters = sentinels.keySet().stream().collect(Collectors.toList());
            List<String> sentinelstrs = new ArrayList<String>();
            sentinels.values().forEach(value -> {
                sentinelstrs.addAll(value);
            });
            for (String sentinel : sentinelstrs) {
                HostAndPort hap = HostAndPort.parseString(sentinel);
                MasterListener masterListener = new MasterListener(masters, hap.getHost(), hap.getPort());
                masterListener.setDaemon(true);
                masterListeners.add(masterListener);
                masterListener.start();
            }
        }
        return shardMasters;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    protected static class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
        private final List<JedisShardInfo> shards;
        private final Hashing algo;
        private final Pattern keyTagPattern;

        public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        public PooledObject<ShardedJedis> makeObject() throws Exception {
            ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
            return new DefaultPooledObject<ShardedJedis>(jedis);
        }

        public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
            final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
            for (Jedis jedis : shardedJedis.getAllShards()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {

                    }
                    jedis.disconnect();
                } catch (Exception e) {

                }
            }
        }

        public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
            try {
                ShardedJedis jedis = pooledShardedJedis.getObject();
                for (Jedis shard : jedis.getAllShards()) {
                    if (!shard.ping().equals("PONG")) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        public void activateObject(PooledObject<ShardedJedis> p) throws Exception {

        }

        public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {

        }
    }

    protected class MasterListener extends Thread {

        protected List<String> masters;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis;
        protected Jedis jedis;
        protected AtomicBoolean running;

        protected MasterListener() {
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
        }

        public MasterListener(List<String> masters, String host, int port) {
            super(String.format("MasterListener-%s-[%s:%d]", masters.stream().collect(Collectors.joining(",")), host, port));
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
            this.masters = masters;
            this.host = host;
            this.port = port;
        }

        public MasterListener(List<String> masters, String host, int port,
                              long subscribeRetryWaitTimeMillis) {
            this(masters, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {
            running.set(true);
            while (running.get()) {
                jedis = new Jedis(host, port);
                try {
                    if (!running.get()) {
                        break;
                    }
                    jedis.subscribe(new JedisPubSub() {
                        public void onMessage(String channel, String message) {
                            log.fine("Sentinel " + host + ":" + port + " published: " + message + ".");
                            String[] switchMasterMsg = message.split(" ");
                            if (switchMasterMsg.length > 3) {
                                int index = masters.indexOf(switchMasterMsg[0]);
                                if (index >= 0) {
                                    HostAndPort newHostMaster = toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                    List<HostAndPort> newHostMasters = new ArrayList<HostAndPort>();
                                    for (int i = 0; i < masters.size(); i++) {
                                        newHostMasters.add(null);
                                    }
                                    Collections.copy(newHostMasters, currentHostMasters);
                                    newHostMasters.set(index, newHostMaster);
                                    initPool(newHostMasters);
                                } else {
                                    String masterNames = masters.stream().collect(Collectors.joining(","));
                                    log.fine("Ignoring message on +switch-master for master name "
                                            + switchMasterMsg[0]
                                            + ", our monitor master name are ["
                                            + masterNames + "]");
                                }

                            } else {
                                log.severe("Invalid message received on Sentinel "
                                        + host
                                        + ":"
                                        + port
                                        + " on channel +switch-master: "
                                        + message);
                            }
                        }
                    }, "+switch-master");

                } catch (JedisConnectionException e) {

                    if (running.get()) {
                        log.severe("Lost connection to Sentinel at " + host
                                + ":" + port
                                + ". Sleeping 5000ms and retrying.");
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        log.fine("Unsubscribing from Sentinel at " + host + ":"
                                + port);
                    }
                } finally {
                    this.jedis.close();
                }
            }
        }

        public void shutdown() {
            try {
                log.fine("Shutting down listener on " + host + ":" + port);
                running.set(false);
                if (this.jedis != null) {
                    this.jedis.disconnect();
                }
            } catch (Exception e) {
                log.severe("Caught exception while shutting down: " + e.getMessage());
            }
        }
    }
}