package owlstone.dbclient.db.module;

/**
 * Created by aj65 on 2016/8/11.
 */
public class Delete extends AlterAction  {

    public Delete(String dataSourceName, String sql) {
        super(dataSourceName, sql);
    }
}
