package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Jedis读写分离，读请求走Slave集群，写请求走Master
 */
public class JedisSentinelSlavePool extends JedisPoolAbstract {


    private final Object initPoolLock;
    private final Object initSlavePoolLock;
    protected Map<String, GenericObjectPool<Jedis>> slavePools = new ConcurrentHashMap<String, GenericObjectPool<Jedis>>();
    protected GenericObjectPoolConfig poolConfig;
    protected int connectionTimeout;
    protected int soTimeout;
    protected String password;
    protected int database;
    protected String clientName;
    protected int sentinelConnectionTimeout;
    protected int sentinelSoTimeout;
    protected String sentinelClientName;
    protected Set<JedisSentinelSlavePool.MasterListener> masterListeners;
    protected org.slf4j.Logger log;
    private volatile JedisFactory factory;
    private volatile HostAndPort currentHostMaster;
    private volatile List<HostAndPort> currentSlaves = Collections.synchronizedList(new ArrayList<HostAndPort>());

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), 2000, null, 0);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), 2000, password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null, 0);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, String password) {
        this(masterName, sentinels, poolConfig, 2000, password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password) {
        this(masterName, sentinels, poolConfig, timeout, password, 0);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password, int database) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password, int database, String clientName) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, int soTimeout, String password, int database) {
        this(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, null);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, int database, String clientName) {
        this(masterName, sentinels, poolConfig, connectionTimeout, soTimeout, password, database, clientName, 2000, 2000,null);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, int database, String clientName, int sentinelConnectionTimeout, int sentinelSoTimeout,  String sentinelClientName) {
        this.masterListeners = new HashSet();
        this.log = LoggerFactory.getLogger(this.getClass().getName());
        this.initPoolLock = new Object();
        this.initSlavePoolLock = new Object();
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.sentinelConnectionTimeout = sentinelConnectionTimeout;
        this.sentinelSoTimeout = sentinelSoTimeout;
        this.sentinelClientName = sentinelClientName;


        Map<String, Object> map = this.initSentinels(sentinels, masterName);
        this.initPool((HostAndPort) map.get("master"));
        List<HostAndPort> slaves = (List<HostAndPort>) map.get("slaves");
        initSlavePools(slaves);
    }

    public void returnBrokenResource(Jedis resource) {
        if (resource != null) {
            this.returnBrokenResourceObject(resource);
        }
    }

    public void returnResource(Jedis resource) {

        if (resource != null) {
            try {
                resource.resetState();
                this.returnResourceObject(resource);
            } catch (Exception var3) {
                this.returnBrokenResource(resource);
                throw new JedisException("Resource is returned to the pool as broken", var3);
            }
        }

    }

    public void destroy() {
        Iterator var1 = this.masterListeners.iterator();

        while (var1.hasNext()) {
            JedisSentinelPool.MasterListener m = (JedisSentinelPool.MasterListener) var1.next();
            m.shutdown();
        }

        super.destroy();
    }

    public HostAndPort getCurrentHostMaster() {
        return this.currentHostMaster;
    }


    public List<HostAndPort> getCurrentSlaves() {
        return currentSlaves;
    }


    private void initPool(HostAndPort master) {
        synchronized (this.initPoolLock) {
            if (!master.equals(this.currentHostMaster)) {
                this.currentHostMaster = master;
                if (this.factory == null) {
                    this.factory = new JedisFactory(master.getHost(), master.getPort(), this.connectionTimeout, this.soTimeout, this.password, this.database, this.clientName);
                    this.initPool(this.poolConfig, this.factory);
                } else {
                    this.factory.setHostAndPort(this.currentHostMaster);
                    this.internalPool.clear();
                }

                this.log.info("Created JedisPool to master at " + master);
            }

        }
    }


    private void addSlavePool(HostAndPort slave) {
        log.info("current redis slaves：" + currentSlaves);
        if (!currentSlaves.contains(slave)) {
            currentSlaves.add(slave);
            log.info("add a redis slave" + slave);
            GenericObjectPool<Jedis> pool = new GenericObjectPool<Jedis>(new JedisFactory(slave.getHost(), slave.getPort(), connectionTimeout,
                    soTimeout, password, database, clientName, false, null, null, null), poolConfig);
            slavePools.put(slave.getHost() + ":" + slave.getPort(), pool);
        } else {
            log.info("redis slave already contain：" + slave);
        }
    }


    private void removeSlavePool(HostAndPort slave) {
        if (currentSlaves.contains(slave)) {
            log.info("remove a redis slave");
            currentSlaves.remove(slave);
            slavePools.remove(slave.getHost() + ":" + slave.getPort());
        } else {
            log.info("redis slave not contain：" + slave);
        }
    }

    public Jedis getSlaveResource() {

        try {
            if (slavePools.size() > 0) {
                Random random = new Random();//随机算法需要改进，https://www.163.com/dy/article/G3CBU72R0511Q1AF.html
                List<String> list = new ArrayList<String>(slavePools.keySet());
                String s = list.get(random.nextInt(list.size()));
                GenericObjectPool<Jedis> pool = slavePools.get(s);
                return pool.borrowObject();
            } else {
                return internalPool.borrowObject();
            }
        } catch (Exception e) {
            throw new JedisConnectionException("Could not get a slave resource from the pool", e);
        }
    }

    private void initSlavePools(List<HostAndPort> slaves) {
        synchronized (this.initSlavePoolLock) {

            if (slaves.size() > 0) {
                currentSlaves = slaves;
                if (this.slavePools != null && this.slavePools.size() > 0) {
                    slavePools.clear();
                }
                log.info("init redis slaves :" + slaves);
                for (HostAndPort slave : slaves) {
                    GenericObjectPool<Jedis> pool = new GenericObjectPool<Jedis>(new JedisFactory(slave.getHost(), slave.getPort(), connectionTimeout,
                            soTimeout, password, database, clientName), poolConfig);
                    slavePools.put(slave.getHost() + ":" + slave.getPort(), pool);
                }
            }

        }
    }

    private Map<String, Object> initSentinels(Set<String> sentinels, String masterName) {
        Map<String, Object> masterSlaves = new HashMap<String, Object>();
        HostAndPort master = null;
        boolean sentinelAvailable = false;
        this.log.info("Trying to find master from available Sentinels...");
        Iterator var5 = sentinels.iterator();

        String sentinel;
        HostAndPort hap;
        while (var5.hasNext()) {
            sentinel = (String) var5.next();
            hap = HostAndPort.parseString(sentinel);
            this.log.debug("Connecting to Sentinel {}", hap);
            Jedis jedis = null;

            try {
                jedis = new Jedis(hap.getHost(), hap.getPort(), this.sentinelConnectionTimeout, this.sentinelSoTimeout);


                if (this.sentinelClientName != null) {
                    jedis.clientSetname(this.sentinelClientName);
                }

                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
                sentinelAvailable = true;
                if (masterAddr != null && masterAddr.size() == 2) {
                    master = this.toHostAndPort(masterAddr);
                    masterSlaves.put("master", master);
                    this.log.debug("Found Redis master at {}", master);
                    List<Map<String, String>> maps = jedis.sentinelSlaves(masterName);
                    if (maps != null && maps.size() > 0) {
                        List<HostAndPort> list = new ArrayList<HostAndPort>();
                        for (Map<String, String> map : maps) {
                            log.info("redis slave：" + map);
                            String host = map.get("ip");
                            String port = map.get("port");
                            String flags = map.get("flags");
                            if (!flags.contains("disconnected") && !flags.contains("s_down")) {
                                HostAndPort hostAndPort = new HostAndPort(host, Integer.parseInt(port));
                                log.info(hostAndPort.toString());
                                list.add(hostAndPort);
                            }
                        }
                        masterSlaves.put("slaves", list);
                    }
                    break;
                }

                this.log.warn("Can not get master addr, master name: {}. Sentinel: {}", masterName, hap);
            } catch (JedisException var13) {
                this.log.warn("Cannot get master address from sentinel running @ {}. Reason: {}. Trying next one.", hap, var13.toString());
            } finally {
                if (jedis != null) {
                    jedis.close();
                }

            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                throw new JedisException("Can connect to sentinel, but " + masterName + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is " + masterName + " master is running...");
            }
        } else {
            this.log.info("Redis master running at " + master + ", starting Sentinel listeners...");
            var5 = sentinels.iterator();

            while (var5.hasNext()) {
                sentinel = (String) var5.next();
                hap = HostAndPort.parseString(sentinel);
                JedisSentinelSlavePool.MasterListener masterListener = new JedisSentinelSlavePool.MasterListener(masterName, hap.getHost(), hap.getPort());
                masterListener.setDaemon(true);
                this.masterListeners.add(masterListener);
                masterListener.start();
            }

            return masterSlaves;
        }
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

    public Jedis getResource() {
        while (true) {
            Jedis jedis = super.getResource();
            jedis.setDataSource(this);
            HostAndPort master = this.currentHostMaster;
            HostAndPort connection = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
            if (master.equals(connection)) {
                return jedis;
            }

            this.returnBrokenResource(jedis);
        }
    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis;
        protected volatile Jedis j;
        protected AtomicBoolean running;


        protected MasterListener() {
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
        }

        public MasterListener(String masterName, String host, int port) {
            super(String.format("MasterListener-%s-[%s:%d]", masterName, host, port));
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port,
                              long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {
            this.running.set(true);

            while (this.running.get()) {
                this.j = new Jedis(this.host, this.port);

                try {
                    if (!this.running.get()) {
                        break;
                    }

                    this.j.subscribe(new JedisPubSub() {
                        public void onMessage(String channel, String message) {
                            JedisSentinelSlavePool.this.log.debug("Sentinel {}:{} published: {}.", MasterListener.this.host, MasterListener.this.port, message);
                            String[] switchMasterMsg = message.split(" ");
                            if (switchMasterMsg.length > 3) {
                                if (channel.equals("+switch-master")) {
                                    switchMaster(switchMasterMsg);
                                } else if (channel.equals("+slave")) {
                                    slave(switchMasterMsg);
                                } else if (channel.equals("+sdown")) {
                                    addSdown(switchMasterMsg);
                                } else if (channel.equals("-sdown")) {
                                    subtractSdown(switchMasterMsg);
                                }
                            } else {
                                JedisSentinelSlavePool.this.log.error("Invalid message received on Sentinel {}:{} on channel +switch-master: {}", MasterListener.this.host, MasterListener.this.port, message);
                            }

                        }
                    }, "+switch-master", "+sdown", "-sdown", "+slave");
                } catch (JedisException var8) {
                    if (this.running.get()) {
                        JedisSentinelSlavePool.this.log.error("Lost connection to Sentinel at {}:{}. Sleeping 5000ms and retrying.", this.host, this.port, var8);

                        try {
                            Thread.sleep(this.subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException var7) {
                            JedisSentinelSlavePool.this.log.error("Sleep interrupted: ", var7);
                        }
                    } else {
                        JedisSentinelSlavePool.this.log.debug("Unsubscribing from Sentinel at {}:{}", this.host, this.port);
                    }
                } finally {
                    this.j.close();
                }
            }
        }

        public void shutdown() {

            try {
                JedisSentinelSlavePool.this.log.debug("Shutting down listener on {}:{}", this.host, this.port);
                this.running.set(false);
                if (this.j != null) {
                    this.j.disconnect();
                }
            } catch (Exception var2) {
                JedisSentinelSlavePool.this.log.error("Caught exception while shutting down: ", var2);
            }
        }


        private void switchMaster(String[] message) {
            if (JedisSentinelSlavePool.MasterListener.this.masterName.equals(message[0])) {
                JedisSentinelSlavePool.this.initPool(JedisSentinelSlavePool.this.toHostAndPort(Arrays.asList(message[3], message[4])));
                removeSlavePool(toHostAndPort(Arrays.asList(message[3], message[4])));
            } else {
                JedisSentinelSlavePool.this.log.debug("Ignoring message on +switch-master for master name {}, our master name is {}", message[0], JedisSentinelSlavePool.MasterListener.this.masterName);
            }
        }


        private void slave(String[] message) {
            HostAndPort hostAndPort = new HostAndPort(message[2], Integer.parseInt(message[3]));
            addSlavePool(hostAndPort);
        }


        private void addSdown(String[] message) {

            if (message[0].equals("slave")) {
                HostAndPort hostAndPort = new HostAndPort(message[2], Integer.parseInt(message[3]));
                log.info("slave:" + hostAndPort + "+sdown");
                removeSlavePool(hostAndPort);
            } else {
                log.info("not slave +sdown message");
            }

        }


        public void subtractSdown(String[] message) {

            if (message[0].equals("slave")) {
                HostAndPort hostAndPort = new HostAndPort(message[2], Integer.parseInt(message[3]));
                log.info("slave:" + hostAndPort + "-sdown");
                addSlavePool(hostAndPort);
            } else {
                log.info("not slave -sdown message");
            }
        }
    }


}