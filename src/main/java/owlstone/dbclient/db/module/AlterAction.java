package owlstone.dbclient.db.module;

/**
 * Created by aj65 on 2016/8/16.
 */
public abstract class AlterAction extends Action {

    public AlterAction(String dataSourceName) {
        super(dataSourceName);
    }

    public AlterAction(String dataSourceName, String sql) {
        super(dataSourceName, sql);
    }
}
