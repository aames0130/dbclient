package owlstone.dbclient.db;

import owlstone.dbclient.Debug;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import owlstone.dbclient.db.data.DeltaDataGenerator;
import owlstone.dbclient.db.data.JobFinishedCallBack;
import owlstone.dbclient.db.data.ValueGenerator;
import owlstone.dbclient.db.module.*;
import owlstone.dbclient.db.module.delta.DeltaData;
import owlstone.dbclient.db.module.delta.DeltaJobInfo;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DBClient {
    class TransactionTempStore {
        Connection conn;
        String dsName;
        DBResult dbResult;

        TransactionTempStore(Connection conn,String dsName){
            this.conn = conn;
            this.dsName = dsName;
        }
    }

    private final static Logger log = LogManager.getLogger(DBClient.class);
    private final static String ROW_NUM = "RowNum";
    private final static int FETCHSIZE = 5120;
    private DBConnectionManager connectionManager;
    private boolean transaction = false;
    private ThreadLocal<TransactionTempStore> tranTemp = new ThreadLocal<>();
    // for deltaXXX method
    private Map<String,AbstractMap.SimpleEntry<DeltaJobInfo,Timer>> deltaJobInfoMap;

    public DBClient(DBConnectionManager connectionManager){
        this.connectionManager = connectionManager;
        this.deltaJobInfoMap = new ConcurrentHashMap<>();
    }

    private void releaseResource(Statement stmt, ResultSet rs){
        if (null != rs)
        {
            try {rs.close();}
            catch (SQLException e) {/* do nothing */}
        }

        if (null != stmt)
        {
            try { stmt.close(); }
            catch (SQLException e) {/* do nothing */}
        }
    }

    private static String getDBName(final Connection conn){
        String dbName = null;
        if( null!=conn )
        {
            try
            {
                String url = conn.getMetaData().getURL();
                dbName = url.split(":")[2].substring(2);
            }
            catch (SQLException e)
            { log.warn("get DB Name of Connection {} fail",conn,e); }
        }

        return dbName;
    }

    /**
     * extract result table information, including each column's type and name
     *
     * @param mt the metadata of result set
     * @return  return an Object array with index 2,
     * first index is a ColumnType array
     * second index is a String array which are column names
     * @throws SQLException throw a SQL exception if mt.getColumnLabel or mt.getPrecision fail
     */
    private static Object[] extractColumnInfo(final ResultSetMetaData mt) throws SQLException{
        Object[] ret = new Object[3];
        final int columnNum = mt.getColumnCount();
        String[]     columns = new String[columnNum];
        ColumnType[] types = new ColumnType[columnNum];
        int[] precision = new int[columnNum];

        ret[0] = types;
        ret[1] = columns;
        ret[2] = precision;

        for (int i = 0; i < columnNum; i++)
        {
            columns[i] = mt.getColumnLabel(i+1);
            types[i]  = ColumnType.fromSQLType(mt.getColumnType(i + 1));
            precision[i] = mt.getPrecision(i+1);
        }

        return ret;
    }

    @SuppressWarnings("unused")
    public long getResultRowNum(String sql, String conPoolName) {
        long ret = -1;
        Statement stmt = null;
        ResultSet rs = null;
        String countSQL = String.format("SELECT COUNT(1) FROM( %s ) AS src",sql);
        Connection conn = connectionManager.getConnection( conPoolName );

        if( null!=conn)
        {
            try
            {
                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs = stmt.executeQuery(countSQL);
                if( rs.first() )
                    ret = rs.getLong(1);
            }
            catch (SQLException sqle)
            {
                log.error("get total row fail.",sqle);
            }

            releaseResource(stmt,rs);
            connectionManager.recycleConnection(conPoolName,conn);
        }

        return ret;
    }

    /**
     * get the max number of primaryKey in executed SQL result
     *
     * @param sql the sql string
     * @param primaryKey  the column name of primary key
     * @param conPoolName the name in delta-config.yaml
     */
    @SuppressWarnings("unused")
    public long getMaxPKValue(String sql, String primaryKey, String conPoolName) {
        //TODO
        // now only support SQLServer, need to support other DB
        long ret = -1;
        StringBuilder maxRowNumSql = new StringBuilder(0xFF);
        maxRowNumSql.append("SELECT MAX(" + ROW_NUM + ") as maxRow from (select ROW_NUMBER() over (order by " + primaryKey + ") as " + ROW_NUM + ",* from(");
        maxRowNumSql.append(sql);
        maxRowNumSql.append(") as NewTable ) as NewTable2 ");

        Connection conn;
        Statement stmt = null;
        ResultSet rs = null;
        conn = connectionManager.getConnection(conPoolName);
        if( null!=conn)
        {
            try
            {
                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs = stmt.executeQuery(maxRowNumSql.toString());
                if(rs.first())
                    ret = rs.getLong(1);
            }
            catch(SQLException sqle)
            {
                log.error("getTotalRowNumber fail ",sqle);
            }

            this.releaseResource(stmt,rs);
            connectionManager.recycleConnection(conPoolName,conn);
        }

        return ret;
    }

    private DBResult query(Query action) {
        String sql = action.getSql();
        Connection conn ;
        Statement stmt = null;
        ResultSet rs = null;
        DBResult ret = new DBResult( action );

        conn = connectionManager.getConnection(action.getDataSourceName());
        if( null != conn)
        {
            try
            {
                ret.setServerName( DBClient.getDBName(conn) );
                stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(FETCHSIZE);

                //
                // get the first resultSet
                // SQL may combined with multi sqls, the first data may create a temp table for speed.
                // the second data do the real query, so we need to use stmt.getMoreResults() to get the first
                // resultSet
                boolean isResultSet = stmt.execute(sql);
                while(!isResultSet && stmt.getUpdateCount()!=-1)
                {  isResultSet = stmt.getMoreResults(); }

                if(isResultSet)
                    rs = stmt.getResultSet();

                if( rs!=null && rs.first() )
                {
                    log.trace("has first row");
                    //
                    // remember column name first
                    //
                    ResultSetMetaData mt = rs.getMetaData();
                    try{ret.setCatelogName(mt.getCatalogName(1));}
                    catch(Exception e){ /* do nothing */}


                    Object[] colInfo = extractColumnInfo(mt);
                    ColumnType[] types = (ColumnType[]) colInfo[0];
                    String[]     columns = (String[]) colInfo[1];
                    int[]        precision = (int[]) colInfo[2];

                    //ret = new DBResult(data,types,columns);
                    ret.setColumnTypes(types);
                    ret.setColumnNames(columns);

                    //
                    // read data in first row
                    // if type is Decimal or DateTime, rs.getString() will truncate the tail '0' value
                    // e.g.  2017-03-30 16:37:33.600 => 2017-03-30 16:37:33.6
                    // use precision to add '0' on the tail
                    //
                    final int columnNum = types.length;
                    String[] values = new String[columnNum];
                    for (int i = 0; i < columnNum; i++)
                    {
                        values[i] = (null != rs.getString(i + 1) ? rs.getString(i + 1).trim() : null);
                        if(types[i] == ColumnType.TIMESTAMP && null!=values[i])
                        {
                            if(values[i].length() < precision[i])
                                values[i] = values[i] + String.format("%0"+(precision[i]-values[i].length())+"d",0);
                            else if(values[i].length() > precision[i])
                                values[i] = values[i].substring(0,precision[i]);
                        }
                    }

                    ret.addRow(values);

                    //
                    // read data in the other rows
                    //
                    while (rs.next())
                    {
                        String[] newRow = new String[columnNum];
                        for (int i = 0; i < columnNum; i++)
                        {
                            newRow[i] = (null != rs.getString(i + 1) ? rs.getString(i + 1).trim() : null);
                            if(types[i] == ColumnType.TIMESTAMP && null!=newRow[i])
                            {
                                if(newRow[i].length() < precision[i])
                                    newRow[i] = newRow[i] + String.format("%0"+(precision[i]-newRow[i].length())+"d",0);
                                else if(newRow[i].length() > precision[i])
                                    newRow[i] = newRow[i].substring(0,precision[i]);
                            }
                        }

                        ret.addRow(newRow);
                    }
                }
                ret.setSuccess();

            }
            catch(SQLException sqle)
            {
                log.error("SQL:< {} >, execute data fail", sql,sqle);
                ret.setException( sqle );
            }
            finally
            {
                releaseResource(stmt,rs);
                connectionManager.recycleConnection( action.getDataSourceName(),conn );
            }
        }
        else
            ret.setException(new Exception("get Connection fail, check error log"));

        return ret;
    }

    private DBResult alter(Action action) {
        char[] escapeChar = this.connectionManager.getManagerConfig().getDBType().getEscapeChar();
        String sql = action.getSql().replace('[',escapeChar[0]).replace(']',escapeChar[1]);
        log.debug("sql = {}",sql);

        Connection conn;
        Statement stmt = null;
        //ResultSet rs = null;

        DBResult ret = new DBResult( action );

        conn = !this.transaction?connectionManager.getConnection(action.getDataSourceName()): tranTemp.get().conn;
        if( null != conn)
        {
            try
            {
                ret.setServerName( DBClient.getDBName(conn) );
                //
                // if is transaction mode and last action fail, the following action should not be executed
                //
                if(transaction && null!=tranTemp.get().dbResult && !tranTemp.get().dbResult.isSuccess() )
                    return ret;

                stmt = conn.createStatement();
                int updateRow = stmt.executeUpdate(sql);
                ret.setSuccess().setUpdatedRows(updateRow);

                ResultSet rs = stmt.getGeneratedKeys();
                List<Long> pks = new ArrayList<>();
                try
                {
                    while (rs.next())
                        pks.add(rs.getLong(1));
                }
                catch (SQLException sqle){/* do nothing */}
                finally
                {
                    try {rs.close(); }
                    catch (SQLException e){/* do nothing */}
                }

                ret.setGeneratedPKList(pks);

            }
            catch(SQLException sqle)
            {
                log.error("SQL:< {} >, execute data fail", sql,sqle);
                ret.setException( sqle );
            }
            finally
            {
                releaseResource(stmt,null);

                if(!transaction)
                    connectionManager.recycleConnection( action.getDataSourceName(),conn );
                else
                    tranTemp.get().dbResult = ret;
            }
        }
        else
            ret.setException(new Exception("get Connection fail, check error log"));

        return ret;
    }

    private DBResult prepareStatement(PStmt stmt) throws SQLException {
        Connection conn;
        if(stmt.isQuery())
        {
            conn = connectionManager.getConnection(stmt.getDataSourceName());
            Debug.log_coon.debug("PStmt is query, so get a general connection");
        }
        else
        {
            Debug.log_coon.debug("PStmt is batch update, so get Transaction connection");
            beginTransaction(stmt.getDataSourceName());
            conn = tranTemp.get().conn;
        }

        DBResult ret = new DBResult( stmt );
        PreparedStatement preparedStatement = null;
        ResultSet rs  = null;

        if( null != conn)
        {
            try
            {
                //
                // if is transaction mode and last action fail, the following action should not be executed
                //
                if(transaction && null!=tranTemp.get().dbResult && !tranTemp.get().dbResult.isSuccess() )
                    return ret;

                preparedStatement = conn.prepareStatement(stmt.getSql(),ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ret.setServerName( DBClient.getDBName(conn) );

                if(!stmt.isQuery())
                {

                    log.debug("Pstmt is batch update, going to update");
                    for(Object[] objects : stmt.getAlterValuesList())
                    {
                        for(int i=0;i<objects.length;i++)
                        {
                            //TODO
                            // Only support VARCHAR now , may support other type in the future.
                            if( null == objects[i])
                                preparedStatement.setNull(i+1, Types.VARCHAR);
                            preparedStatement.setObject(i+1,objects[i]);
                        }
                        preparedStatement.addBatch();
                    }

                    int[] updateRows = preparedStatement.executeBatch();
                    int updateCount = 0;
                    for(int i : updateRows)
                        updateCount += i;
                    ret.setUpdatedRows(updateCount);

                    ResultSet rs_update = preparedStatement.getGeneratedKeys();
                    List<Long> pks = new ArrayList<>();
                    try
                    {
                        while (rs_update.next())
                            pks.add(rs_update.getLong(1));
                    }
                    catch (SQLException sqle){/* do nothing*/}
                    finally
                    {
                        try {rs_update.close(); }
                        catch (SQLException e){/* do nothing*/}
                    }

                    ret.setGeneratedPKList(pks);

                }
                else
                {
                    log.debug("Pstmt is query, going to query ");

                    preparedStatement.setFetchSize(FETCHSIZE);
                    List<Object> queryValues = stmt.getQueryValues();
                    for(int i=0;i<queryValues.size();i++)
                    {
                        //TODO
                        // Only support VARCHAR now , may support other type in the future.
                        if(null == queryValues.get(i))
                            preparedStatement.setNull(i+1,Types.VARCHAR);
                        preparedStatement.setObject(i + 1, queryValues.get(i));
                    }

                    rs = preparedStatement.executeQuery();

                    if( rs!=null && rs.first() )
                    {
                        log.trace("has first row");
                        //
                        // remember column name first
                        //
                        ResultSetMetaData mt = rs.getMetaData();
                        try { ret.setCatelogName(mt.getCatalogName(1)); }
                        catch(Exception e){ /* do nothing */}

                        Object[] colInfo = extractColumnInfo(mt);
                        ColumnType[] types = (ColumnType[]) colInfo[0];
                        String[]     columns = (String[]) colInfo[1];
                        int[]        precision = (int[]) colInfo[2];

                        //ret = new DBResult(data,types,columns);
                        ret.setColumnTypes(types);
                        ret.setColumnNames(columns);

                        //
                        // read data in first row
                        // if type is Decimal or DateTime, rs.getString() will truncate the tail '0' value
                        // e.g.  2017-03-30 16:37:33.600 => 2017-03-30 16:37:33.6
                        // use precision to add '0' on the tail
                        //
                        final int columnNum = types.length;
                        String[] values = new String[columnNum];
                        for (int i = 0; i < columnNum; i++)
                        {
                            values[i] = (null != rs.getString(i + 1) ? rs.getString(i + 1).trim() : null);
                            if(types[i] == ColumnType.TIMESTAMP && null!=values[i])
                            {
                                if(values[i].length() < precision[i])
                                    values[i] = values[i] + String.format("%0"+(precision[i]-values[i].length())+"d",0);
                                else if(values[i].length() > precision[i])
                                    values[i] = values[i].substring(0,precision[i]);
                            }
                        }

                        ret.addRow(values);

                        //
                        // read data in the other rows
                        //
                        while (rs.next())
                        {
                            String[] newRow = new String[columnNum];
                            for (int i = 0; i < columnNum; i++)
                            {
                                newRow[i] = (null != rs.getString(i + 1) ? rs.getString(i + 1).trim() : null);
                                if(types[i] == ColumnType.TIMESTAMP && null!=newRow[i])
                                {
                                    if(newRow[i].length() < precision[i])
                                        newRow[i] = newRow[i] + String.format("%0"+(precision[i]-newRow[i].length())+"d",0);
                                    else if(newRow[i].length() > precision[i])
                                        newRow[i] = newRow[i].substring(0,precision[i]);
                                }
                            }

                            ret.addRow(newRow);
                        }
                    }
                }

                ret.setSuccess();
            }
            catch(SQLException sqle)
            {
                log.error("Thread {} SQL:< {} >, execute data fail",Thread.currentThread().hashCode(), stmt.getSql(),sqle);
                ret.setException( sqle );
            }
            finally
            {
                releaseResource(preparedStatement,rs);
                if(!transaction)
                    connectionManager.recycleConnection( stmt.getDataSourceName(),conn );
                else
                    tranTemp.get().dbResult = ret;

                if(!stmt.isQuery())
                {
                    log.info("Pstmt is batch update, now endTransaction()");
                    ret = endTransaction();

                    //TODO check
                    /*
                     * In case lose exception message, if something error in SQL.
                     */
//                    if( null == ret.getException() )
//                      ret = endTransaction();
//                    else
//                        endTransaction();
                }
            }
        }
        else
            ret.setException(new Exception("get Connection fail, check error log"));

        return ret;
    }

    public DBResult execute(Action action){
        DBResult ret = new DBResult( action );
        if(action instanceof Query)
            ret = this.query((Query)action);
        else if(action instanceof Insert || action instanceof Update || action instanceof  Delete)
            ret = this.alter(action);
        else if(action instanceof PStmt)
        {
            try { ret = this.prepareStatement((PStmt) action); }
            catch (SQLException e) { ret.setFail().setException(e); }
        }

        return ret;
    }

    /**
     * Execute a list of AlterAction.
     * Because there are lots of update, can only save the latest Action into DBResult.
     * @param actionList a AlterAction List
     * @return DBResult, only
     */
    public DBResult multiAlter(String dataSource,List<AlterAction> actionList) {
        char[] escapeChar = this.connectionManager.getManagerConfig().getDBType().getEscapeChar();

        Connection conn;
        DBResult ret = new DBResult(actionList.get(actionList.size()-1));
        //
        // if is transaction mode and last action fail, the following action should not be executed
        //
        if(transaction && null!=tranTemp.get().dbResult && !tranTemp.get().dbResult.isSuccess())
            return ret;

        conn = !this.transaction?connectionManager.getConnection(dataSource): tranTemp.get().conn;

        if(null != conn)
        {
            ret.setServerName( DBClient.getDBName(conn) );
            Statement stmt = null;
            for(Action action:actionList)
            {
                try
                {
                    stmt = conn.createStatement();
                    int updateRow = stmt.executeUpdate(action.getSql().replace('[',escapeChar[0]).replace(']',escapeChar[1]));
                    ret.setSuccess().setUpdatedRows(ret.getUpdatedRows()+updateRow);
                }
                catch (SQLException sqle)
                {
                    log.error("SQL:< {} >, execute data fail", ret.getAction().getSql(),sqle);
                    if(!this.transaction)
                        ret.setException( sqle );
                    else
                    {
                        //
                        // in Transaction mode, once fail, not to do next action
                        //
                        break;
                    }
                }
                finally
                {
                    // even break, this finally block will be executed.

                    releaseResource(stmt,null);
                    if(this.transaction)
                    {
                        log.debug("this.transaction and success");
                        tranTemp.get().dbResult = ret;
                    }
                }
            }
        }
        else
            ret.setException(new Exception("get Connection fail, check error log"));

        if(!transaction)
            connectionManager.recycleConnection( dataSource,conn );

        return ret;
    }

    public void beginTransaction(String dsName) throws SQLException {
        this.endTransaction(); // force rollback old first
        Connection conn = connectionManager.getConnection(dsName,false);
        conn.setAutoCommit(false);

        TransactionTempStore transactionTempStore = new TransactionTempStore(conn,dsName);
        tranTemp.set(transactionTempStore);
        transaction = true;
    }

    public DBResult endTransaction(){
        log.traceEntry();
        TransactionTempStore transactionTempStore = tranTemp.get();
        DBResult ret = new DBResult(null);
        if( null!=transactionTempStore )
        {
            log.debug("null!=transactionTempStore");
            String dsName = transactionTempStore.dsName;
            Connection conn = transactionTempStore.conn;
            ret =  transactionTempStore.dbResult;
            try
            {
                if(ret.isSuccess())
                {
                    log.debug("After beginTransaction() all actions success, so call commit()");
                    conn.commit();
                }
                else
                {
                    log.debug("After beginTransaction() some actions fail, so call rollback()");
                    conn.rollback();
                }
            }
            catch (SQLException e)
            {
                log.debug("!!!! Some exception occurs, when commit() or rollback()");
                ret.setFail().setException(e);
                if(null!=conn)
                {
                    try {conn.rollback();log.info("!!!!!! execute rollback inside SQLException()");}
                    catch(SQLException e1) { log.error("rollback fail",e1); }
                }
            }
            finally
            {
                try { conn.setAutoCommit(true); }
                catch (SQLException e) {/*do nothing*/}

                connectionManager.recycleConnection(dsName,conn);
                tranTemp.set(null);
                transaction = false;
            }
        }

        return ret;
    }

    public boolean inTransaction() {
        return this.transaction;
    }

    public DeltaJobInfo deltaStart(InputStream is) {
        return this.deltaStart(is,null);
    }

    public DeltaJobInfo deltaStart(InputStream is,JobFinishedCallBack callBack){
        DeltaJobInfo ret = new DeltaJobInfo();

        Constructor constructor = new Constructor();
        constructor.addTypeDescription(new TypeDescription(DeltaData.class, "!delta-data"));

        DeltaData deltaData;
        try
        {
            Yaml yaml = new Yaml(constructor);
            deltaData = (DeltaData) yaml.load(is);
            // parse zoneId
            final String ZONEIDREGEX = "EST|HST|MST|ACT|AET|AGT|ART|AST|BET|BST|CAT|CNT|CST|CTT|EAT|ECT|IET|IST|JST|MIT|NET|NST|PLT|PNT|PRT|PST|SST|VST|UTC";

            if(null!=deltaData.getZoneId() && !deltaData.getZoneId().matches(ZONEIDREGEX))
            {
                ret.setException(new IOException(deltaData.getZoneId() + " is not valid zoneId."));
                log.error("DeltaData's zoneId {} is invalid",deltaData.getZoneId());
                return ret;
            }

            // parse values and create DeltaDataGenerator
            if(deltaData.getColumns().length != deltaData.getValues().length)
            {
                ret.setException(new IOException("The number of column should equals to the number of value"));
                return ret;
            }

            //
            // check if job with the same name is running
            //
            if(this.deltaJobInfoMap.containsKey(deltaData.getName()))
            {
                if(this.deltaJobInfoMap.get(deltaData.getName()).getKey().isRunning())
                {
                    ret.setException(new Exception("DeltaJob with name " + deltaData.getName() + " is running, you must stop it first."));
                    return ret;
                }
                else
                    this.deltaJobInfoMap.remove(deltaData.getName());   // the job is stopped, so we remove it.
            }

            log.info("load Delta data {} success",deltaData.getName());
        }
        catch (Exception e)
        {
            log.error("Load DeltaData config file {} fail, check your file format is correct !",e );
            ret.setException(e);
            return ret;
        }

        //
        // load Delta data config file success
        //
        ret.setDeltaData(deltaData);

        final String PKREGEX = "\\$PK_INT|\\$PK_INT\\(\\d+\\)";
        final String INTREGEX = "\\$VAR_INT\\+?|\\$VAR_INT\\|[-+]?\\d+(,[-+]?\\d+)+\\||\\$VAR_INT\\|[-+]?\\d+~[-+]?\\d+\\|";
        final String FLTREGEX = "\\$VAR_FLT\\+?|\\$VAR_FLT\\|[-+]?\\d+\\.\\d+(,[-+]?\\d+\\.\\d+)+\\|";
        final String STRREGEX = "\\$VAR_STR|\\$VAR_STR\\(\\d+\\)|\\$VAR_STR\\|\\S+(,\\S+)+\\|";
        final String TIMEREGEX = "\\$VAR_TIME|" +
                "\\$VAR_TIME\\|\\d\\d\\d\\d-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T(00|[0-9]|1[0-9]|2[0-3]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])~now\\||" +
                "\\$VAR_TIME\\|now-\\d+[smhd]~now\\|";

        final String whiteSpace ="\\s";
        final String FKSQLREGEX = "\\$VAR_SQL\\|(?i)SELECT"+whiteSpace+"(\\w)+(.)*"+whiteSpace+"FROM(.)+\\|";

        String type;
        List<ValueGenerator> vgList = new ArrayList<>();
        for(Object deltaValue : deltaData.getValues())
        {
            type = deltaValue.getClass().getTypeName();
            log.debug("{} with type={}",deltaValue,type);
            if( type.equals( String.class.getTypeName() ) )
            {
                String vStr = deltaValue.toString();
                if(vStr.charAt(0) == '$')
                {
                    if(vStr.startsWith("$PK_INT"))
                    {
                        if(!vStr.matches(PKREGEX))
                        {
                            ret.setException(new IOException(vStr + " : value format is invalid"));
                            return ret;
                        }

                        if(vStr.equals("$PK_INT"))
                            vgList.add(new ValueGenerator.PKIntGenerator());
                        else
                        {
                            int index = vStr.indexOf("(");
                            vgList.add(new ValueGenerator.PKIntGenerator(Integer.valueOf(vStr.substring(index+1,vStr.length()-1))));
                        }
                    }
                    else if(vStr.startsWith("$VAR_INT"))   // $VAR_INT
                    {
                        if(!vStr.matches(INTREGEX))
                        {
                            ret.setException(new IOException(vStr + " : value format is invalid"));
                            return ret;
                        }

                        // pass validation
                        if(vStr.equals("$VAR_INT"))
                            vgList.add(new ValueGenerator.IntGenerator());
                        else if(vStr.equals("$VAR_INT+"))
                            vgList.add(new ValueGenerator.IntGenerator(true));
                        else if(!vStr.contains(","))  // e.g. $VARINT|1~10|
                        {
                            int index1 = vStr.indexOf("|");
                            int index2 = vStr.indexOf("~");
                            vgList.add(new ValueGenerator.IntGenerator(Integer.valueOf(vStr.substring(index1+1,index2)),
                                    Integer.valueOf(vStr.substring(index2+1,vStr.length()-1) )));
                        }
                        else
                        {
                            int index1 = vStr.indexOf("|");
                            String[] pv = vStr.substring(index1+1,vStr.length()-1).split(",");
                            List<Object> possibleValue = new ArrayList<>();
                            for(String s:pv)
                                possibleValue.add(Integer.valueOf(s));
                            vgList.add( new ValueGenerator.LimitSetGenerator(possibleValue) );
                        }
                    }
                    else if(vStr.startsWith("$VAR_FLT"))  // $VAR_FLT
                    {
                        if(!vStr.matches(FLTREGEX))
                        {
                            ret.setException(new IOException(vStr + " : value format is invalid"));
                            return ret;
                        }

                        if(vStr.equals("$VAR_FLT"))
                            vgList.add(new ValueGenerator.FloatGenerator());
                        if(vStr.equals("$VAR_FLT+"))
                            vgList.add(new ValueGenerator.FloatGenerator(true));
                        else
                        {
                            int index1 = vStr.indexOf("|");
                            String[] pv = vStr.substring(index1+1,vStr.length()-1).split(",");
                            List<Object> possibleValue = new ArrayList<>();
                            for(String s:pv)
                                possibleValue.add(Float.valueOf(s));
                            vgList.add( new ValueGenerator.LimitSetGenerator(possibleValue) );
                        }
                    }
                    else if(vStr.startsWith("$VAR_STR"))  // $VAR_STR
                    {
                        if(!vStr.matches(STRREGEX))
                        {
                            ret.setException(new IOException(vStr + " : value format is invalid"));
                            return ret;
                        }

                        if(vStr.equals("$VAR_STR"))
                            vgList.add(new ValueGenerator.StrGenerator());
                        else if(vStr.charAt(8)=='(')
                        {
                            int num = Integer.parseInt(vStr.substring(9,vStr.length()-1));
                            vgList.add(new ValueGenerator.StrGenerator((byte)num));
                        }
                        else
                        {
                            int index1 = vStr.indexOf("|");
                            String[] pv = vStr.substring(index1+1,vStr.length()-1).split(",");
                            List<Object> possibleValue = new ArrayList<>();

                            //for(String s:pv)
                            //     possibleValue.add(s);
                            possibleValue.addAll(Arrays.asList(pv));
                            vgList.add( new ValueGenerator.LimitSetGenerator(possibleValue) );
                        }
                    }
                    else if(vStr.startsWith("$VAR_TIME"))
                    {
                        if(!vStr.matches(TIMEREGEX))
                        {
                            ret.setException(new IOException(vStr + " : time format is invalid"));
                            return ret;
                        }

                        if(vStr.equals("$VAR_TIME"))
                            vgList.add(new ValueGenerator.DateTimeGenerator(deltaData.getZoneId()));
                        else
                        {
                            //split beginDate and endDate
                            String tempStr = vStr.substring(vStr.indexOf("|")+1,vStr.length()-1);
                            int index = tempStr.indexOf("~");
                            vgList.add(new ValueGenerator.DateTimeGenerator(deltaData.getZoneId(),tempStr.substring(0,index),
                                    tempStr.substring(index+1)));
                        }
                    }
                    else if(vStr.startsWith("$VAR_SQL"))
                    {
                        if(!vStr.matches(FKSQLREGEX))
                        {
                            ret.setException(new IOException(vStr + " : SQL format is invalid. e.g. 'SELECT CustomerNumber FROM dbo.NewEggCustomer' "));
                            return ret;
                        }

                        int updateCount = (int)Math.ceil(deltaData.getDuration()* (deltaData.getNumber()-1)/ValueGenerator.FKValueGenerator.UPDATE_SEC);
                        updateCount = updateCount==0?updateCount+1:updateCount;
                        log.debug("updateCount={}",updateCount);
                        String sql = vStr.substring(vStr.indexOf("|")+1,vStr.length()-1);
                        vgList.add(new ValueGenerator.FKValueGenerator(this,deltaData.getDs(),
                                sql,updateCount));
                    }
                    else
                    {
                        ret.setException(new IOException(vStr + " : value format is invalid, $VAR should start with  $PK_INT," +
                                "$VAR_INT, $VAR_FLT, $VAR_STR, $VAR_TIME or $VAR_SQL" ));
                        return ret;
                    }
                }
                else
                {
                    vgList.add(new ValueGenerator.FixGenerator(vStr));
                }
            }
            else
            {
                //log.debug("vStr {} is float or integer",deltaValue);
                //if(deltaData.getClass().getTypeName().equals(Integer.class.getTypeName()))
                vgList.add(new ValueGenerator.FixGenerator(deltaData));
            }
        }

        //
        // build DeltaDataGenerator and put into deltaJobInfoMap
        //
        DeltaDataGenerator generator = new DeltaDataGenerator(this, deltaData, vgList,callBack);
        Timer timer = new Timer(true);
        timer.schedule(generator,0,(int)(deltaData.getDuration()*1000));
        ret.setRunning(true);
        this.deltaJobInfoMap.put(deltaData.getName(),new AbstractMap.SimpleEntry<>(ret,timer));

        return ret;
    }

    public DeltaJobInfo deltaStop(String name){
        DeltaJobInfo ret = new DeltaJobInfo();
        if(this.deltaJobInfoMap.containsKey(name))
        {
            if(this.deltaJobInfoMap.get(name).getKey().isRunning())
            {
                this.deltaJobInfoMap.get(name).getValue().cancel();
                this.deltaJobInfoMap.get(name).getKey().setRunning(false);
                log.info("stop delta job {} success.",name);
            }
            else
                ret.setException(new Exception("delta job '"+name+"' is not running now"));
        }
        else
            ret.setException(new Exception("There is no job with name '"+name+"'"));

        return ret;
    }
}
