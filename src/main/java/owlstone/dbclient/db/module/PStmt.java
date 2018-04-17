package owlstone.dbclient.db.module;

import java.util.*;

public class PStmt extends Action{
    private List<Object[]> alterValuesList = new LinkedList<>();
    private List<Object>   queryValues;

    private PStmt(String connName,String sql, Object[] params){
        super(connName,sql);
        if( null!=params && params.length > 0)
        {
            queryValues = new ArrayList<>();
            queryValues.addAll(Arrays.asList(params));
        }
    }

    private PStmt(String connName,String sql, List<Object[]> params){
        super(connName,sql);
        if( null!=params && params.size() > 0)
            alterValuesList.addAll(params);
           // params.forEach( values-> alterValuesList.add(values) );
    }


    /**
     * Create a prepareStatement for Query
     * @param connName
     * @param sql
     * @param params
     */
    public static PStmt buildQueryBean(String connName, String sql, Object[] params){
        return new PStmt(connName,sql,params);
    }


    /**
     * Create a prepareStatement for Update, insert or delete
     * @param connName
     * @param sql
     * @param params
     */
    public static PStmt buildBatchUpdateBean(String connName, String sql, List<Object[]> params){
        return new PStmt(connName,sql,params);
    }

    public List<Object[]> getAlterValuesList() {
        return Collections.unmodifiableList(alterValuesList);
    }

    public List getQueryValues() {
        return Collections.unmodifiableList(queryValues);
    }

    public boolean isQuery(){
        return (null!=queryValues && !queryValues.isEmpty());
    }
}
