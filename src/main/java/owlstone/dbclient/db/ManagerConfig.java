package owlstone.dbclient.db;

/**
 * Java bean of config file(Default is dbclient-config.yaml)
 */
public class ManagerConfig {
    public static class DataSource{
        private String name;
        private String url;
        private String username;
        private String passwd;
        private int    poolSize;

        public DataSource() {
        }

        public DataSource(String name, String url, String username, String passwd) {
            this.name = name;
            this.url = url;
            this.username = username;
            this.passwd = passwd;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPasswd() {
            return passwd;
        }

        public void setPasswd(String passwd) {
            this.passwd = passwd;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }
    }

    private owlstone.dbclient.db.module.DBType DBType;
    private int maxThreadNum;
    private int checkPeriod;
    private int checkDelay;
    private DataSource[] dataSourceList;


    public ManagerConfig(){
        checkDelay = 60*5;   // Default is 5 min
        checkPeriod = 60;    // Default is 1 min
    }


    public owlstone.dbclient.db.module.DBType getDBType() {
        return DBType;
    }

    public void setDBType(owlstone.dbclient.db.module.DBType DBType) {
        this.DBType = DBType;
    }

    public DataSource[] getDataSourceList() {
        return dataSourceList;
    }

    public void setDataSourceList(DataSource[] dataSourceList) {
        this.dataSourceList = dataSourceList;
    }

    public int getMaxThreadNum() {
        return maxThreadNum;
    }

    public void setMaxThreadNum(int maxThreadNum) {
        this.maxThreadNum = maxThreadNum<1?1:maxThreadNum;
    }

    public int getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(int checkPeriod) {
        this.checkPeriod = checkPeriod>0?checkPeriod:this.checkDelay;
    }

    public int getCheckDelay() {
        return checkDelay;
    }

    public void setCheckDelay(int checkDelay) {
        this.checkDelay = checkDelay>0?checkDelay:this.checkDelay;
    }

    public DataSource getDataSourceByName(final String name){
        DataSource ret = null;
        for(DataSource ds : dataSourceList)
        {
            if(ds.getName().equals(name))
            {
                ret = ds;
                break;
            }
        }

        return ret;
    }
}
