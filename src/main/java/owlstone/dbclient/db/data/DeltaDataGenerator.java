package owlstone.dbclient.db.data;

import owlstone.dbclient.db.DBClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import owlstone.dbclient.db.module.DBResult;
import owlstone.dbclient.db.module.Insert;
import owlstone.dbclient.db.module.delta.DeltaData;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DeltaDataGenerator extends TimerTask implements IDataGenerator {
    private final static Logger log = LogManager.getLogger(DeltaDataGenerator.class);
    private DBClient dbClient;
    private DeltaData deltaData;
    private List<ValueGenerator> generatorList;
    private AtomicInteger count;
    private JobFinishedCallBack callBackFunction;

    public DeltaDataGenerator(DBClient dbClient, DeltaData deltaData, List<ValueGenerator> generatorList,JobFinishedCallBack callBack) {
        assert(deltaData.getColumns()!=null && generatorList!=null);
        assert(deltaData.getColumns().length == generatorList.size());
        this.dbClient = dbClient;
        this.deltaData = deltaData;
        this.generatorList = generatorList;
        this.count = new AtomicInteger(deltaData.getNumber());
        this.callBackFunction = callBack;
    }

    @Override
    public DBResult generate() {
        Map<String,Object> cvMap = new HashMap<>();
        String[] columns = deltaData.getColumns();

        for(int i=0;i<columns.length;i++)
            cvMap.put(columns[i],generatorList.get(i).generate());

        final Insert insert = new Insert(deltaData.getDs(),deltaData.getTable(),cvMap);
        return dbClient.execute(insert);
    }

    @Override
    public void run() {
        //TODO put result in someplace for check
        DBResult result = this.generate();
        log.debug("SQL={}",result.getAction().getSql());
        log.debug("result.isSuccess()={}",result.isSuccess());

        if(0==count.decrementAndGet())
        {
            log.info("job {} finished !! ",this.deltaData.getName());
            dbClient.deltaStop(deltaData.getName());
            if(null!=this.callBackFunction)
            {
                log.debug("callBackFunction is not null, going to invoke....");
                this.callBackFunction.invoke();
            }
        }
    }
}
