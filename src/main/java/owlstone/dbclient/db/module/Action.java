package owlstone.dbclient.db.module;

/**
 * Created by aj65 on 2016/8/9.
 */
public abstract class Action {
    protected String dataSourceName;
    protected String sql;

    public Action(String dataSourceName){
        this.dataSourceName = dataSourceName;
    }

    public Action(String dataSourceName, String sql){
        this.sql = sql;
        this.dataSourceName = dataSourceName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }
}
