package owlstone.dbclient.db.module;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aj65 on 2016/8/9.
 */
public class Insert extends AlterAction {
    private final static Logger log = LogManager.getLogger(Insert.class);
    private String table;
    private Map<String,Object> data;

    public Insert(String datSourceName, String table) {
        super(datSourceName);
        this.table = table;
        this.data = new HashMap<>();
    }

    public Insert(String datSourceName, String table,Map<String,Object> data){
        super(datSourceName);
        this.table = table;
        this.setData(data);
    }

    /**
     * @return empty if construct by Insert(String dataSourceName, String data)
     */
    public Map<String, Object> getData() {
        return this.data;
    }

    public void setData(Map<String, Object> insertData) {
        if(null!=insertData)
        {
            this.data = insertData;

            StringBuilder sb = new StringBuilder(0x400);
            StringBuilder cos = new StringBuilder(0xFF);
            StringBuilder vls = new StringBuilder(0xFF);

            for(Map.Entry<String,Object> entry: insertData.entrySet())
            {
                cos.append("[").append(entry.getKey()).append("],");
                String clazzName = entry.getValue().getClass().getSimpleName();

                switch (clazzName)
                {
                    case "Integer":
                    case "Long":
                    case "Float":
                    case "Double":
                    case "BigDecimal":
                        vls.append(entry.getValue()).append(",");
                        break;
                    case "Date":
                    case "LocalDateTime":
                        vls.append("'").append(entry.getValue()).append("',");
                        break;
                    case "String":
                    case "Character":
                        if(!entry.getValue().equals("CURRENT_TIMESTAMP") && !entry.getValue().equals("NOW()"))
                            vls.append("'").append(entry.getValue()).append("',");
                        else
                            vls.append(entry.getValue()).append(",");
                        break;
                    case "ZonedDateTime":
                        vls.append("'").append( ((java.time.ZonedDateTime)entry.getValue()).toLocalDateTime()).append("',");
                        break;
                    default:
                        log.error("Unsupported type : {}",clazzName);
                        break;
                }
            }

            sb.append("INSERT INTO ").append(table)
                    .append("(").append(cos.substring(0,cos.length()-1)).append(")")
                    .append("VALUES(").append(vls.substring(0,vls.length()-1)).append(")");

            this.setSql(sb.toString());
            log.debug("After setData() SQL = {}",this.sql);
        }
    }
}
