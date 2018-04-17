package owlstone.dbclient.db.module;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aj65 on 2016/8/11.
 */
public class Update extends AlterAction {
    private final static Logger log = LogManager.getLogger(Update.class);
    private String table;
    private Map<String,Object> data;
    private String sqlCond;  // where condition in SQL

    public Update(String dataSourceName, String table) {
        super(dataSourceName);
        this.table = table;
        this.data = new HashMap<>();
    }

    public Update(String dataSourceName, String table, Map<String,Object> updateData,String cond) {
        super(dataSourceName);
        this.table = table;
        this.data = new HashMap<>();
        this.sqlCond = cond;
        this.setData(updateData);
    }

    public Update(String dataSourceName, String table, Map<String,Object> updateData) {
        this(dataSourceName,table,updateData,null);
    }

    /**
     * @return empty if construct by Update(String dataSourceName, String data)
     */
    public Map<String, Object> getData() {
        return this.data;
    }

    public void setData(Map<String, Object> updateData) {
        if(null!=updateData)
        {
            this.data = updateData;

            StringBuilder sb = new StringBuilder(0x400);
            sb.append("UPDATE ").append(table).append(" ").append("SET ");

            for(Map.Entry<String,Object> entry: updateData.entrySet())
            {
                if(!entry.getKey().startsWith("["))
                    sb.append("[").append(entry.getKey()).append("]").append("=");
                else
                    sb.append(entry.getKey()).append("=");

                String clazzName = entry.getValue().getClass().getSimpleName();

                switch (clazzName)
                {
                    case "Integer":
                    case "Long":
                    case "Float":
                    case "Double":
                    case "BigDecimal":
                        sb.append(entry.getValue()).append(",");
                        break;
                    case "Date":
                    case "LocalDateTime":
                        sb.append("'").append(entry.getValue()).append("',");
                        break;
                    case "String":
                    case "Character":
                        if(!entry.getValue().equals("CURRENT_TIMESTAMP") && !entry.getValue().equals("NOW()"))
                            sb.append("'").append(entry.getValue()).append("',");
                        else
                            sb.append(entry.getValue()).append(",");
                        break;
                    case "ZonedDateTime":
                        sb.append("'").append( ((java.time.ZonedDateTime)entry.getValue()).toLocalDateTime()).append("',");
                        break;
                    default:
                        log.error("Unsupported type : {}",clazzName);
                        break;
                }
            }

            this.setSql( sb.substring(0,sb.length()-1) + ((null==sqlCond)?"":" WHERE "+sqlCond) );
            log.debug("After setData() SQL = {}",this.sql);
        }
    }

    public void setDataAndCond(Map<String, Object> updateData, String cond){
        this.sqlCond = cond;
        this.setData(updateData);
    }
}
