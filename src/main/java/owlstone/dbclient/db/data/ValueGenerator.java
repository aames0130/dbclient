package owlstone.dbclient.db.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import owlstone.dbclient.db.DBClient;
import owlstone.dbclient.db.module.DBResult;
import owlstone.dbclient.db.module.Query;
import owlstone.dbclient.db.module.Row;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public interface ValueGenerator {
    Object generate();

    class FixGenerator implements ValueGenerator{
        private Object value;
        public FixGenerator(final Object v){
            this.value = v;
        }
        @Override
        public Object generate() {
            return value;
        }
    }

    class PKIntGenerator implements ValueGenerator{
        private AtomicInteger atomicInteger;
        public PKIntGenerator(){
            this(1);
        }

        public PKIntGenerator(int start){
            atomicInteger = new AtomicInteger(start);
        }

        @Override
        public Integer generate() {
            return atomicInteger.getAndIncrement();
        }
    }

    class IntGenerator implements ValueGenerator{
        private Random random;
        private int minValue = Integer.MIN_VALUE;
        private int maxValue;
        private boolean positive;

        public IntGenerator(){
            random = new Random();
        }

        public IntGenerator(boolean positive){
            random = new Random();
            this.positive = positive;
        }

        public IntGenerator(final int x,final int y){
            assert(y>x);
            this.minValue = x;
            this.maxValue = y;

            random = new Random();
        }

        @Override
        public Integer generate() {
            int x;
            if( minValue==Integer.MIN_VALUE )
                x = random.nextInt();
            else
                x = random.nextInt(maxValue-minValue)+minValue ;

            return positive?Math.abs(x):x;
        }
    }

    class FloatGenerator implements ValueGenerator{
        private Random random;
        private boolean positive;

        public FloatGenerator(){
            this.random = new Random();
        }

        public FloatGenerator(boolean positive){
            this.random = new Random();
            this.positive = positive;
        }

        @Override
        public Float generate() {
            float f = random.nextFloat();
            return  positive? Math.abs(f):f ;
        }
    }

    class StrGenerator implements ValueGenerator{
        private byte maxLen = 5;
        private Random random;

        public StrGenerator(){
            this.random = new Random();
        }

        public StrGenerator(byte maxLen){
            this.random = new Random();
            this.maxLen = maxLen > 0 ? maxLen:this.maxLen;
        }

        @Override
        public String generate() {
            String ret;

            char[] tempStr = new char[maxLen];
            for(int i=0;i<maxLen;i++)
            {
                int x = random.nextInt(52)+65;
                if(x<97 && x>90)
                    x = x+6;
                tempStr[i] = (char)x;
            }
            ret = new String(tempStr);

            return ret;
        }
    }

    class LimitSetGenerator implements ValueGenerator{
        private Random random;
        private ArrayList<Object> possibleValues;

        public LimitSetGenerator(final List<Object> values){
            this.random = new Random();
            this.possibleValues = new ArrayList<>(values);
        }

        @Override
        public Object generate() {
            return possibleValues.get(random.nextInt(possibleValues.size()));
        }
    }

    class DateTimeGenerator implements ValueGenerator{
        private final static Logger log = LogManager.getLogger(DateTimeGenerator.class);

        private Random random;
        private ZoneId zoneId;
        private ZonedDateTime beginDateTime;
        private ZonedDateTime endDateTime;
        private boolean calculateBegin;
        private boolean calculateEnd;
        private String beginStr;
        private String endStr;
        private long duration;
        private Timer updateTimer;

        private static ZonedDateTime parse(String str, ZoneId zoneId){
            ZonedDateTime ret = null;
            ZonedDateTime now = ZonedDateTime.now(zoneId);

            if(!str.startsWith("now"))
                ret = ZonedDateTime.parse(str);
            else
            {
                if(str.equals("now"))
                    ret = now;
                else
                {
                    String distance = str.substring(str.indexOf("-")+1);
                    int digit = Integer.valueOf(distance.substring(0,distance.length()-1));
                    char unit = distance.charAt(distance.length()-1);
                    log.debug("digit={}, unit={}",digit,unit);
                    switch (unit)
                    {
                        case 's':
                            ret = now.minusSeconds(digit);
                            break;
                        case 'm':
                            ret = now.minusMinutes(digit);
                            break;
                        case 'h':
                            ret = now.minusHours(digit);
                            break;
                        case 'd':
                            ret = now.minusDays(digit);
                            break;
                    }
                }
            }

            log.debug("parse ret ={}",ret);
            return ret;
        }

        public DateTimeGenerator(String zoneId){
            this.random = new Random();
            if(null==zoneId)
                this.zoneId  = Clock.systemDefaultZone().getZone();
            else
                this.zoneId = zoneId.equals("UTC")?ZoneId.of("UTC"):ZoneId.of( ZoneId.SHORT_IDS.get(zoneId) );
        }

        public DateTimeGenerator(String zone,String bDate,String eDate){
            this(zone);

            this.beginStr = bDate;
            this.endStr = eDate;
            this.calculateBegin = bDate.contains("now");
            this.calculateEnd = eDate.contains("now");

            this.beginDateTime = DateTimeGenerator.parse( bDate,this.zoneId );
            this.endDateTime = DateTimeGenerator.parse( eDate,this.zoneId );
            this.duration = this.endDateTime.toInstant().getEpochSecond() - this.beginDateTime.toInstant().getEpochSecond();

            if(this.calculateBegin || this.calculateEnd)
            {
                this.updateTimer = new Timer();
                this.updateTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if(calculateBegin)
                            beginDateTime = DateTimeGenerator.parse( beginStr,zoneId );

                        if(calculateEnd)
                            endDateTime = DateTimeGenerator.parse(endStr,zoneId);

                        duration = endDateTime.toInstant().getEpochSecond() - beginDateTime.toInstant().getEpochSecond();
                        log.debug("beginDateTime={}  endDateTime={}",beginDateTime,endDateTime);
                        log.debug("update ....duration={}",duration);
                    }
                },60*1000,60*1000);
            }
        }

        @Override
        public Object generate() {
            ZonedDateTime ret;
            long x = this.duration==0?Math.abs(random.nextLong()):Math.abs(random.nextLong()%duration);
            log.debug("x={}",x);
            // x (valid values -999999999 - 999999999)
            x = x%1000000000;

            if(null!=beginDateTime)
                ret = beginDateTime.plusSeconds(x);
            else
                ret = ZonedDateTime.of(LocalDateTime.ofEpochSecond(x,0, ZoneOffset.UTC),this.zoneId);

            log.debug("generate ret={}",ret);
            return ret;
        }
    }


    /**
     * Generate column value according to SQL execute result,
     * usually used on generate FK value reference to other table's PK.
     *
     * It will auto update possible values every 5 minutes.
     */
    class FKValueGenerator implements ValueGenerator{
        private final static Logger log = LogManager.getLogger( FKValueGenerator.class );
        public final static int UPDATE_SEC = 5*60;

        private Random random;
        private Timer updateTimer;
        private DBClient dbClient;
        private String ds;
        private String sql;
        private AtomicInteger updateCount;
        private ReentrantLock lock;

        private List<Object> possibleValueList;


        public FKValueGenerator(DBClient dbClient,final String ds,final String sql,final int updateCount){
            this.random = new Random();
            this.updateTimer = new Timer();
            this.dbClient  = dbClient;
            this.ds = ds;
            this.sql = sql;
            this.updateCount = new AtomicInteger(updateCount);

            this.lock = new ReentrantLock();
            this.possibleValueList = new ArrayList<>();

            scheduleUpdateValue();
        }

        private void updateValue(){
            lock.lock();

            log.debug("update value......");
            try
            {
                DBResult result = dbClient.execute(new Query(ds,sql));
                log.debug("query result = {}",result.isSuccess());
                if(result.isSuccess() && result.getRowSize() > 0)
                {
                    possibleValueList.clear();
                    log.debug("result are {} possible value",result.getRowSize());
                    for(Row row: result.getRowList())
                    {
                        log.debug("value = {}:{}",row.getValues()[0],row.getTypes()[0]);
                        switch(row.getTypes()[0])
                        {
                            case BIT:
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                                possibleValueList.add( Long.valueOf(row.getValues()[0]) );
                                break;
                            case FLOAT:
                            case REAL:
                            case DOUBLE:
                            case NUMERIC:
                            case DECIMAL:
                                possibleValueList.add( Double.valueOf(row.getValues()[0]) );
                                break;
                            default:
                                possibleValueList.add( String.valueOf(row.getValues()[0]) );
                        }
                    }
                }
            }
            finally { lock.unlock();}
        }

        private void scheduleUpdateValue(){
            updateTimer.schedule(new TimerTask() {
                @Override
                    public void run() {
                    updateValue();
                    if(updateCount.decrementAndGet()==0)
                        updateTimer.cancel();
                }
            },0, UPDATE_SEC *1000);
        }

        @Override
        public Object generate() {
            Object ret;
            lock.lock();
            try
            {
                int index = random.nextInt(possibleValueList.size());
                ret =  possibleValueList.get(index);
            }
            finally {lock.unlock();}

            return ret;
        }
    }
}
