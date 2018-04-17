package owlstone.dbclient.db.module;

import java.util.*;

/**
 * This class wrap the result of DBClient query, alter...etc.
 */
public class DBResult{
    private boolean success;
    private String ServerName;
    private String catelogName;
    private Action action;
    private List<Row>      rowList;
    private String[]       columnNames;
    private ColumnType[]   columnTypes;
    private int updatedRows;  // for update, delete
    private List<Long> generatedPKList;
    private Exception exception;

    public DBResult(Action action,ColumnType[] types, String[] names) {
        assert(types!=null && columnNames!=null);
        assert(types.length == columnNames.length);

        rowList = new ArrayList<>();
        generatedPKList = new ArrayList<>();
        this.action = action;
        this.columnNames = names;
        this.columnTypes = types;
    }

    public DBResult(Action action) {
        this.action = action;
        rowList = new ArrayList<>();
        generatedPKList = new ArrayList<>();
    }

    public DBResult merge(DBResult... dbResults){
        if( null!=this.columnTypes && this.columnTypes.length >0 )
        {
            // only input parameter match original result, then we do merge
            for(DBResult dbResult:dbResults)
            {
                if(null!=dbResult.getColumnTypes())
                {
                    if(java.util.Arrays.toString(this.columnNames).equals(java.util.Arrays.toString(dbResult.getColumnNames())))
                        dbResult.getRowList().forEach(r->this.rowList.add(r));
                }
            }
        }

        return this;
    }

//    public DBResult(ResultSet resultSet){
//
//
//    }

    public Action getAction() {
        return action;
    }

    public List<Row> getRowList() {
        return rowList;
    }

    private String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public Row getRow(int index){
        return rowList.get(index);
    }

    public boolean isSuccess() {
        return success;
    }

    public DBResult setSuccess() {
        this.success = true;
        return this;
    }

    public DBResult setFail(){
        this.success = false;
        return this;
    }

    public void setRowList(List<Row> rowList) {
        this.rowList = rowList;
    }

    private ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(ColumnType[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    /**
     * @param name  the name of column , case sensitive
     * @return return true if find column with the given name ; otherwise false
     */
    public boolean hasColumn(String name){
        boolean ret = false;
        if(null!=columnNames)
        {
            for(String n:columnNames)
            {
                if(n.equals(name))
                {
                    ret =true;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * always use this method to add new row , DON'T use getRowList() to get a List to add new row
     * @param values  the values in new row
     */
    public void addRow(String[] values)
    {
        if(values!=null && values.length == columnNames.length)
        {
            Row row = new Row(this.columnTypes,this.columnNames,values);
            rowList.add(row);
        }
    }

    public Row.Cell getCell(int rowIndex,int columnIndex){
        return getRow(rowIndex).getCell(columnIndex);
    }

    public Row.Cell getCell(int rowIndex,String columnName){
        return getRow(rowIndex).getCell(columnName);
    }

    public int getRowSize(){
        return rowList.size();
    }

    public int getColumnSize(){
        return columnNames!=null?columnNames.length:0;
    }

    public int getUpdatedRows() {
        return updatedRows;
    }

    public void setUpdatedRows(int updatedRows) {
        this.updatedRows = updatedRows;
    }

    public List<Long> getGeneratedPKList() {
        return Collections.unmodifiableList(this.generatedPKList);
    }

    public void setGeneratedPKList(List<Long> generatedPKList) {
        this.generatedPKList = generatedPKList;
    }

    public String getServerName() {
        return ServerName;
    }

    public void setServerName(String serverName) {
        this.ServerName = serverName;
    }

    public String getCatelogName() {
        return catelogName;
    }

    public void setCatelogName(String catelogName) {
        this.catelogName = catelogName;
    }

    public String toJson(){
        StringBuilder sb = new StringBuilder(0xFF);
        sb.append("[");
          for(Row row:this.rowList)
          {
              sb.append("{");
              for(int i=0;i<columnNames.length;i++)
              {
                  sb.append("\"").append(columnNames[i]).append("\":");
                  if(null==row.getValues()[i])
                  {
                      sb.append("null,");
                      continue;
                  }

                  if(columnTypes[i]==ColumnType.BIT || columnTypes[i]==ColumnType.TINYINT || columnTypes[i]==ColumnType.SMALLINT ||
                     columnTypes[i]==ColumnType.INTEGER || columnTypes[i]==ColumnType.BIGINT || columnTypes[i]==ColumnType.FLOAT ||
                     columnTypes[i]==ColumnType.REAL || columnTypes[i]==ColumnType.DOUBLE || columnTypes[i]==ColumnType.NUMERIC ||
                     columnTypes[i]==ColumnType.DECIMAL || columnTypes[i]==ColumnType.NULL || columnTypes[i]==ColumnType.BOOLEAN)
                  {
                      sb.append(row.getValues()[i]);
                  }
                  else
                  {
                      sb.append("\"").append(row.getValues()[i]).append("\"");
                  }

                  sb.append(",");
              }
              sb.deleteCharAt(sb.length()-1);

              sb.append("},");
          }
          if(sb.length()>1)
           sb.deleteCharAt(sb.length()-1);
        sb.append("]");
        return sb.toString();
    }

    public List<Map<String,String>> toListMap(){
        List<Map<String,String>> ret = new LinkedList<>();
        rowList.forEach(r->ret.add(r.toMap()));
        return ret;
    }

    @Override
    public String toString(){
        return this.toJson();
    }
}
