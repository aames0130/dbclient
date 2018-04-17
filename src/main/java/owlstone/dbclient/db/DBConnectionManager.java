package owlstone.dbclient.db;

import owlstone.dbclient.Debug;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.yaml.snakeyaml.constructor.Constructor;

public class DBConnectionManager extends AutoRecycleConnectionManager {
    private final static Logger log = LogManager.getLogger( DBConnectionManager.class );
    private final static String CONFIG_FILE_NAME = "dbclient-config.yaml";

    private final Random random = new Random();
    private static class Lock { }

    class ConnectionWrap{
        private final static int NAMESIZE = 3;
        private String name;
        private Connection connection;
        private AtomicInteger threadNum;  // how many threads are using this Connection
        private int maxSharedThreadNum;   // the max number of threads that can share this Connection
        private boolean share;            // if false, only one thread can use this connection in the same time
        private LocalDateTime lastAccessTime;

        ConnectionWrap(final String name,int maxSharedThreadNum, final Connection connection) {
            this.name = name;
            this.maxSharedThreadNum = maxSharedThreadNum;
            this.connection = connection;
            this.threadNum = new AtomicInteger();
            this.share = true;
            this.lastAccessTime = LocalDateTime.now();
        }

        Connection getConnectionAndIncUsedThreadNum(boolean share) {
            threadNum.incrementAndGet();
            this.share = share;
            return connection;
        }

        private void backConnectionAndDecUsedThreadNum(final Connection conn){
            if(this.connection == conn)
            {
                threadNum.decrementAndGet();
                share = true;
                Debug.log_coon.debug("ConnectionWrap.backConnectionAndDecUsedThreadNum()\n  return connection of {} to ConnectionWrap success, {}",name,this);
            }
        }

        private int getUsedThreadNum(){
            return threadNum.get();
        }

        private boolean isShare() {
            return share;
        }

        public String getName() {
            return name;
        }

        public Connection getConnection() {
            return connection;
        }

        private boolean isReachMaxUsedThreadNum(){
            return threadNum.get()==maxSharedThreadNum;
        }

        private boolean isConnAlive(){
            boolean ret = false;
            if( null!=connection )
            {
                try { ret = connection.isValid(500); }
                catch (SQLException e) { /*do nothing */ }
            }
            return ret;
        }

        @Override
        public String toString() {
            return String.format("{\"name\":\"%s\",\"threadNum\":%d,\"maxSharedThreadNum\":%d,\"share\":%s," +
                    "\"lastAccessTime\":\"%s\"}",name,threadNum.get(),maxSharedThreadNum,share,lastAccessTime);
        }
    }

    private Map<String,List<ConnectionWrap>> connectionPoolMap;
    private final Object lockGetConn = new Lock();

    private ManagerConfig readConfig(InputStream inputStream) {
        ManagerConfig ret;

        Constructor constructor = new Constructor();
        constructor.addTypeDescription(new TypeDescription(ManagerConfig.class, "!dbclient-config"));

        Yaml yaml = new Yaml(constructor);
        try
        {
            ret = (ManagerConfig) yaml.load(inputStream);
            inputStream.close();
        }
        catch (Exception e)
        {
            log.error("load ManagerConfig from inputStream fail !",e );
            ret = null;
        }

        return ret;
    }

    private Connection getDBConnection(final String url, final Properties connectionProps, StringBuilder errMsg){
        Debug.log_coon.traceEntry("getDBConnection() url={}, connectionProps={}",url,connectionProps);
        Connection conn = null;
        int retry = 3;
        int pause = 300;
        while( retry-->0 )
        {
            try
            {
                conn = DriverManager.getConnection(url,connectionProps);
                break;
            }
            catch (SQLException e)
            {
                try{ Thread.currentThread().sleep(pause*=2); }
                catch (InterruptedException e1) {/* do nothing */}
                errMsg.append(e.getMessage());
                Debug.log_coon.warn("get Connection fail",e);
            }
        }
        Debug.log_coon.traceExit("getDBConnection()");
        return conn;
    }

    private String randomName(int len){
        char[] name = new char[len];
        int range = 122-97+1;
        for(int i=0;i<len;i++)
            name[i] = (char)(random.nextInt(range)+97);

        return new String(name);
    }

    private ConnectionWrap buildConnectionWrap(String dsName,Connection conn){
        return new ConnectionWrap(dsName+"::" + randomName(ConnectionWrap.NAMESIZE),
                managerConfig.getMaxThreadNum(),conn);
    }

    private Map<String,List<ConnectionWrap>> initConnPool(ManagerConfig managerConfig){
        Debug.log_coon.trace("================================");
        Debug.log_coon.traceEntry("initConnPool({})",managerConfig);
        Map<String,List<ConnectionWrap>> ret = new ConcurrentHashMap<>();
        String jdbcClassName = managerConfig.getDBType().getJdbcDriver();
        try
        {
            Class.forName(jdbcClassName);
            for( ManagerConfig.DataSource ds : managerConfig.getDataSourceList() )
            {
                final int poolSize = ds.getPoolSize();
                List<ConnectionWrap> connWraps = new LinkedList<>();
                ret.put(ds.getName(),connWraps);

                Properties connectionProps = new Properties();
                connectionProps.put("user", ds.getUsername());
                connectionProps.put("password", ds.getPasswd());

                for(int i=0;i<poolSize;i++)
                {
                    //
                    // only if get connection successfully, connWraps[i] has instance
                    // otherwise it will be null
                    //
                    StringBuilder stringBuilder = new StringBuilder();
                    Connection conn = getDBConnection(ds.getUrl(),connectionProps,stringBuilder);
                    if(null != conn)
                    {
                        ConnectionWrap connectionWrap = buildConnectionWrap(ds.getName(),conn);
                        connWraps.add(connectionWrap);
                        Debug.log_coon.debug("set DB connection to ConnectionWrap {} of DataSource {} success",
                                connectionWrap.getName(), ds.getName());
                    }
                    else
                        Debug.log_coon.error("get Connection from DB of DataSource {} fail, msg={}",
                                ds.getName(),stringBuilder.toString() );
                }
            }
        }
        catch (ClassNotFoundException e)
        {   log.error("Load Class {} fail",jdbcClassName,e);    }

        Debug.log_coon.traceExit("initConnPool()");
        return ret;
    }

    /**
     * Default constructor, will load file 'dbclient-config.yaml' in class path to init managerConfig
     */
    public DBConnectionManager(){
        this.connectionPoolMap = new ConcurrentHashMap<>();
        URL url = Thread.currentThread().getContextClassLoader().getResource(CONFIG_FILE_NAME);
        if(null!=url)
        {
            try
            {
                InputStream is = url.openStream();
                this.managerConfig = readConfig(url.openStream());
            }
            catch (IOException e) {/* do nothing */}
        }

        if( null != this.managerConfig )
            this.connectionPoolMap = initConnPool(this.managerConfig);

        this.startCheck();
    }

    /**
     * Constructor to load your own file config file to init managerConfig
     */
    public DBConnectionManager(final InputStream inputStream){
        this.connectionPoolMap = new ConcurrentHashMap<>();

        this.managerConfig = readConfig(inputStream);
        if( null != this.managerConfig )
            this.connectionPoolMap = initConnPool(this.managerConfig);

        this.startCheck();
    }

    /**
     * get free Connection from Pool, if all connection exceed its max share thread count, we
     * will get a new connection from DB
     */
    @Override
    public Connection getConnection(String dsName,boolean share){
        Debug.log_coon.traceEntry("Thread {} getConnection({},{})",Thread.currentThread().hashCode(),dsName,share);

        Connection conn = null;
        List<ConnectionWrap> poolList = connectionPoolMap.get(dsName);
        ManagerConfig.DataSource dataSource = managerConfig.getDataSourceByName(dsName);

        if(null != dataSource && null != poolList)
        {
            synchronized (lockGetConn)
            {
                Properties connectionProps = new Properties();
                connectionProps.put("user", dataSource.getUsername());
                connectionProps.put("password", dataSource.getPasswd());

                poolList.sort(Comparator.comparing(ConnectionWrap::getUsedThreadNum));
                // find the first ConnectionWrap that eligible
                // 1. not share
                // 2. not exceed ManagerConfig.maxThreadNum
                Debug.log_coon.debug("Thread {} going to get a {} connection",
                        Thread.currentThread().hashCode(),share?"shared":"monopolize");
                for(ConnectionWrap connectionWrap: poolList)
                {
                    if(!connectionWrap.isShare())
                        continue;
                    Debug.log_coon.debug("  connWrap {} is share and has {} thread used it now",
                            connectionWrap.getName(), connectionWrap.getUsedThreadNum());
                    if(!connectionWrap.isReachMaxUsedThreadNum())
                    {
                        //
                        // if we need to get a share connection, only need to check if shareThreadNum exceed MaxUsedThreadNum
                        // if we need to get a monopolize connection, can only get a no one used connectionWrap.
                        //
                        if(share)
                            conn = connectionWrap.getConnectionAndIncUsedThreadNum(share);
                        else if(connectionWrap.getUsedThreadNum()==0)
                            conn = connectionWrap.getConnectionAndIncUsedThreadNum(share);

                        if(null!=conn)
                        {
                            Debug.log_coon.info("Thread {} got a {} Connection in connWrap {}",
                                    Thread.currentThread().hashCode(),share?"shared":"monopolize",connectionWrap.getName());
                            break;
                        }
                    }
                }

                if( null == conn)
                {
                    Debug.log_coon.info("All connection in pool reach the thread limit or are monopolize or we need monopolize connection, \n  we need to get a new Connection from DB now.");
                    StringBuilder errMsg = new StringBuilder(0x255);
                    Connection newConn = getDBConnection(dataSource.getUrl(),connectionProps,errMsg);
                    if( null!=newConn )
                    {
                        ConnectionWrap connectionWrap = buildConnectionWrap(dsName,newConn);
                        poolList.add(connectionWrap);
                        conn = connectionWrap.getConnectionAndIncUsedThreadNum(share);
                        Debug.log_coon.info("Thread {} got a new Connection from DB and put to wrap {}, " +
                                        "ConnectionWrap pool size is {} now.",
                                Thread.currentThread().hashCode(),connectionWrap.getName(),poolList.size());
                    }
                    else
                    {
                        log.error("get new Connection from DB fail, msg = {}",errMsg);
                    }
                }
            }
        }
        Debug.log_coon.trace("Thread {} Exit getConnection()",Thread.currentThread().hashCode());
        return conn;
    }

    /**
     * recycle a connection to connection pool
     * @param dsName dataSource name
     * @param connection the DB connection
     */
    @Override
    public void recycleConnection(String dsName,Connection connection) {
        Debug.log_coon.traceEntry("recycleConnection() Thread {} going to recycle connection of pool {}",Thread.currentThread().hashCode(),dsName);
        List<ConnectionWrap> connectionWraps = connectionPoolMap.get(dsName);
        if( null!=connectionWraps )
        {

            for(ConnectionWrap connwrap: connectionWraps)
            {
                if(connection.equals(connwrap.getConnection()))
                {
                    connwrap.backConnectionAndDecUsedThreadNum(connection);
                    break;
                }
            }
        }
    }

    /**
     * close all connections in connection pool and clean this pool
     * @param dsName the name of connection pool
     */
    @Override
    public void removeDBConn(String dsName) {
        Debug.log_coon.entry(dsName);
        List<ConnectionWrap> connectionWraps = this.connectionPoolMap.get(dsName);
        synchronized (lockGetConn)
        {
            if( null != connectionWraps )
            {
                Debug.log_coon.debug("pool of {} not null, going to close its connections", dsName);
                connectionWraps.forEach(connWrap -> {
                    try {
                        connWrap.getConnection().close();
                    } catch (SQLException e) { /* do nothing*/}
                });

                connectionWraps.clear();
            }
        }
    }

    @Override
    public void removeAllDBConn() {
        for( ManagerConfig.DataSource ds: this.managerConfig.getDataSourceList() )
            this.removeDBConn(ds.getName());
    }

    /**
     * remove dead connections in Pool and resupply new connections to pool
     */
    @Override
    protected void removeDeadConn() {
        Debug.log_coon.trace("====== Trigger removeExpireAndDeadConn() at {} to remove dead connections ======"
                ,java.time.LocalTime.now());
        synchronized (lockGetConn)
        {
            for( Map.Entry<String,List<ConnectionWrap>> connPool : connectionPoolMap.entrySet() )
            {
                List<ConnectionWrap> removeList = new ArrayList<>();
                List<ConnectionWrap> connectionWraps = connPool.getValue();
                String poolName = connPool.getKey();

                Debug.log_coon.info("before removeExpireAndDeadConn of DataSource {}, its size = {}",poolName,connectionWraps.size());

                //
                // remove dead connections
                //
                connectionWraps.forEach(connwrap -> {
                    Debug.log_coon.debug("{} , connwrap.getUsedThreadNum()= {} , connwrap.isConnAlive()= {}",
                            connwrap.getName(), connwrap.getUsedThreadNum(),connwrap.isConnAlive());

                    if(!connwrap.isConnAlive())
                    {
                        try { connwrap.getConnection().close();}
                        catch (SQLException e) { /* do nothing*/}
                        removeList.add(connwrap);

                        Debug.log_coon.debug("connwrap {} add to removeList because conn.isAlive() return false, \n {}"
                                ,connwrap.getName(),connwrap);
                    }
                });

                connectionWraps.removeAll(removeList);
                Debug.log_coon.info("after connectionWraps.removeAll(removeList) of DataSource {}, its size = {}",poolName,connectionWraps.size());

                //
                // resupply connection
                //
                ManagerConfig.DataSource ds = managerConfig.getDataSourceByName(poolName);
                Properties connectionProps = new Properties();
                connectionProps.put("user", ds.getUsername());
                connectionProps.put("password", ds.getPasswd());

                int poolsize_org = ds.getPoolSize();
                int poolsize_now = connectionWraps.size();
                Debug.log_coon.info("poolsize_org={} , poolsize_now = {}",poolsize_org,poolsize_now);

                for(int i=0;i< poolsize_org - poolsize_now;i++)
                {
                    Connection conn = getDBConnection(ds.getUrl(),connectionProps,new StringBuilder());
                    if( null!=conn)
                        connectionWraps.add(buildConnectionWrap(poolName,conn));
                }

                Debug.log_coon.info("After resupply connection of DataSource {} , it size = {}",poolName,connectionWraps.size());
            }
        }

        Debug.log_coon.traceExit();
    }

    /**
     * @param dsName
     * @return  -1 connectionPoolMap has no key of dsName
     */
    public int getConnPoolSize(String dsName){
        int ret = -1;
        List<ConnectionWrap> connwrapList = this.connectionPoolMap.get(dsName);
        ret = null!=connwrapList?connwrapList.size():ret;
        return ret;
    }

    public String getConnPoolInfo(){
        StringBuilder sb = new StringBuilder(0x1FF);
        sb.append("{");
        connectionPoolMap.entrySet().forEach( map->
                sb.append("\"").append(map.getKey()).append("\":").append(map.getValue()).append(",")
        );
        sb.deleteCharAt(sb.length()-1);
        sb.append("}");
        return sb.toString();
    }
}
