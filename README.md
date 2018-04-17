# DBClient

| Branch        | Coverage       |
|:-------------:|:--------------:|
| master        |[![coverage report](http://ST01MB01:8085/module/dbclient/badges/master/coverage.svg?private_token=ry-xV_k5zCbCyN-AVQoD)](http://ST01MB01:8085/module/dbclient/commits/master)|
| dev           |[![coverage report](http://ST01MB01:8085/module/dbclient/badges/dev/coverage.svg?private_token=ry-xV_k5zCbCyN-AVQoD)](http://ST01MB01:8085/module/dbclient/commits/dev)|

DBClient make basic DB operation easier, including query, insert, update, delete and prepare statement.  
You don't have to worry about DB connection management and exception handleing.  
Try it !!  

## Basic Usage
### 1. add dependency to your pom.xml
```xml
<dependencies>
  <dependency>
    <groupId>owlstone</groupId>
    <artifactId>dbclient</artifactId>
    <version>1.1.2</version>
  </dependency>
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>sqljdbc4</artifactId>
    <version>4.0</version>
  </dependency>
</dependencies>
```
The example above is MS SQL-Server.  
If you use different Database, just dependent its JDBC driver.  
DBClient support MS SQL-Server, MySQL and Maria DB now.  

### 2. write config file dbclient-config.yaml
```yaml
!dbclient-config
DBType: SQLServer
maxThreadNum: 20
#checkDelay: 300
#checkPeriod: 60
dataSourceList:
  - name: DB1
    url: 'jdbc:sqlserver://ST02CPS04'
    username: bigdata
    passwd: 0123456
    poolSize: 3
  - name: DB2
    url: 'jdbc:sqlserver://ST02CPS03'
    username: sa
    passwd: Bigdata0123456
    poolSize: 3
```

DBType can be SQLServer, MySQL or MariaDB.  
DB connections will be shared among threads unless you get a transaction connection.  
maxThreadNum means the max thread number can share a connection, bigger value will lead less resource usage,  
while smaller value will bring better performance. You should config this value according to your situation.  

DBClient will auto remove dead connections, the check period is configurable.  
It will start check 5 min (300 secs) later after ConnectionManager was created and check one time every minute(60 secs) by default.  
You can set check delay and check period by providing the value of checkDelay and checkPeriod in config file.    

Define different DB connections under dataSourceList.  
The name of each data source should be unique,  poolSize for connection pool size of each data source.  

### 3. write log4j2.xml
### 4. located dbclient-config.yaml and log4j2.xml on java -cp
### 5. Basic API
Use DBConnectionManager to management DB connections.  
DBConnectionManager will pre-get DB connections and put them in its pool.  
Use DBConnectionManager.getConnection() to get connection from its pool.  

Construct DBClient instance and pass DBConnectionManager instance to its constructor.  
You should have only ONE DBConnectionManager instance in your project unless you need to connect to different kind of DB Servers in your project.  
```java
/* use default config file 'dbclient-config.yaml' */
DBConnectionManager dbConnectionManager = new DBConnectionManager();

/* 
 * DBConnectionManager will load file dbclient-config.yaml as config file by default.  
 * If you want to use other file or your config file is on the web, you should read file as InputStream and pass it to Constructor.  
 */ 

// dbMag1 = new DBConnectionManager(Thread.currentThread().getContextClassLoader().getResourceAsStream('mysql-config.yaml')) 
DBClient dbclient = new DBClient(dbConnectionManager);
```

DBClient.execute(Action action).  
Action class is the super class of Query, Insert, Update, Delete and PStmt.  
PStmt is for PrepareStatement, you can use it for query or batch update, insert or delete.  
If you want to query, create a Query bena, if you want to insert, ceate a Insert bean, and so on.  

#### Query 
* Query(String connName,String sql)  
   sql is general SQL

#### Insert  
* insert(String datSourceName, String table, Map<String,Object> data)  
   Map<String,Object> data : key is column name and value is insert value 
* insert(String datSourceName, String table)  
   need call setData() later 
  
#### Update  
* Update(String dataSourceName, String table, Map<String,Object> updateData,String cond) 
   Map<String,Object> updateData: key is column name and value is update value. cond is sql after 'WHERE'  
* Update(String dataSourceName, String table, Map<String,Object> updateData)  
   the same with above except cond is null  
* Update(String dataSourceName, String table)  
   need call setData() or setDataAndCond later  

#### Delete  
* Delete(String dataSourceName, String sql)
   sql is general SQL  

#### PStmt  
* PStmt.buildQueryBean(String connName, String sql, Object[] params)  
* PStmt.buildBatchUpdateBean(String connName, String sql, List<Object[]> params)  

### 6. Sample Code
```java        
        final String dsName = "DB1";
        final String tableName = "dbo.Customer";       
        DBClient dbClient = new DBClient(dbMag1);
		
		// query
        DBResult result_query =  dbClient.execute( new Query(dsName,"SELECT * FROM dbo.Customer") );
        
        // insert
        Insert insertAction = new Insert(dsName,tableName);
        Map<String,Object> data = new HashMap();
        data.put("FirstName","Aames");
        data.put("LastName","Jiang");
        data.put("Age",18);
        data.put("Phone","28825252");
        insertAction.setData(data);
        DBResult result_insert = dbClient.execute(insertAction);
        
        //update
        Update update = new Update(dsName,tableName);
        Map<String,Object> data = new HashMap<>();
        data.put("Phone","3939889");
        update.setDataAndCond(data,"FirstName = 'Aames'");
        DBResult result_update = dbClient.execute(update);        
        
        //delete
        final String SQL = "DELETE FROM dbo.Customer WHERE customerNumber = 100";
        Delete delete = new Delete(dsName,SQL);
        DBResult result_delete = dbClient.execute(delete);

        //PStmt for query
        PStmt queryBean = PStmt.buildQueryBean(dsName,"SELECT * FROM dbo.Customer WHERE customerNumber = ?",new Object[]{1});
        DBResult result_psquery = dbClient.execute(queryBean);

        //PStmt for batch insert
        final String batchinsert_SQL = "insert into dbo.Customer(`FirstName`,`LastName`,`Age`,`Phone`) values (?,?,?,?)";        
        List<Object[]> values = new ArrayList();
        values.add(new Object[]{"Alice","Huang",22,"1234567"});
        values.add(new Object[]{"Bob","Chang",36,"9876543"});
        values.add(new Object[]{"Christin","Young",29,"7777777"});
        PStmt batchInsertBean = PStmt.buildBatchUpdateBean(dsName,batchinsert_SQL,values);
        DBResult result_psbatch = dbClient.execute(batchInsertBean);
```

If you need to do lots of updates to different Tables, you can use method multiAlter.  
multiAlter(String dataSource,List<AlterAction> actionList)  
This method accepts a list of AlterAction, it will iterate this list and execute it by order.    
```java
        final String dsName = "DB1";
        DBConnectionManager dbMag = new DBConnectionManager();
        DBClient dbClient = new DBClient(dbMag);
        Map<String,Object> data1 = new HashMap<>();
        Map<String,Object> data2 = new HashMap<>();
        data1.put("id",1);
        data1.put("name","bigdata");
        
        data2.put("id",1);
        data2.put("name","Aames");
        data2.put("deptid",1);
        data2.put("id",2);
        data2.put("name","Amanda");
        data2.put("deptid",1);
       
        Insert insert1 = new Insert(dsName,"testdb.dept",data1);
        Insert insert2 = new Insert(dsName,"testdb.employee",data2);
 
        DBResult dbResult = null;
        try {
            dbClient.beginTransaction(dsName);
            dbClient.multiAlter(dsName,Arrays.asList(new AlterAction[]{insert1,insert2}));
            dbResult = dbClient.endTransaction();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        if( null != dbResult )        
            log.debug("Result is {}",dbResult.toJson()); 
```

### 7. Transaction
DBClient support transaction.  
You can invoke beginTransaction("DSName") before any insert/update/delete and invoke endTransaction() in the end.  
If there is any exception occurs, it will rollback automatically.  
beginTransaction("DSName") and endTransaction() is thread dependent, so you can invoke beginTransaction("DSName") in one method and invoke endTransaction() in the other method, but must in the same thread.  
Method endTransaction will return the final result, check the result there.  
```java
    final String dsName = "DB1";
    String tableName = "testdb.employee";
    DBConnectionManager dbMag = new DBConnectionManager();
    DBClient dbClient = new DBClient(dbMag);

    Insert insert1 = new Insert(dsName,tableName);
    Insert insert2 = new Insert(dsName,tableName);
    Insert insert3 = new Insert(dsName,tableName);
        
    Map<String,Object> data1 = new HashMap<>();        
    data1.put("id",3);
    data1.put("name","Alice");
    data1.put("deptid",1);
    
    Map<String,Object> data2 = new HashMap<>();
    data2.put("id",4);
    data2.put("name","Bob");
    data2.put("deptid",99);  // deptid is ref to table testdb.dept that has no id 99, so insert will fail
 
    
    Map<String,Object> data3 = new HashMap();
    data3.put("id",5);
    data3.put("name","Christine");
    data3.put("deptid",1);
    
    insert1.setData(data1);
    insert2.setData(data2);
    insert3.setData(data3);
    
    DBResult result = null;
    try {
        dbClient.beginTransaction(MYSQLDB);            
        dbClient.execute(insert1);
        dbClient.execute(insert2);
        dbClient.execute(insert3);
        result = client.endTransaction();
    } catch (SQLException e) {
        e.printStackTrace();
    }
    
    log.debug("isSuccess = {}",result.isSuccess());
    log.debug("update row num = {}",result.getUpdatedRows());        
```
### 8. DBResult
The return type of execute() and multiAlter() and endTransaction() is DBResult, all data and returned information are wrapped in it.   
The main APIs of DBResult:  
* boolean isSuccess()
* int getRowSize()
* Row getRow(int index)
* List<Row> getRowList()
* int getUpdatedRows()
* String toJson()
* List<Map<String,String>> toListMap()
* public Exception getException()
* merge(DBResult... dbResults)
 
If action executes success, DBResult.isSuccess() will be true and you can get the rows number by DBResult.getRowSize().  
DBResult.getRowList() will return a list of Row.  
A Row is combined of Cells and there are column type, name and value information in a Cell.  
  
You can simply call toString() to show the result in JSON String format.  
DBResult.toString() is equivalent to DBResult.toJson().  
DBResult.toListMap() will return a List that has multi HashMap, and the Map key is column name and value is column's value.  
You can merge multi DBResults into one DBResult but a premise is they have the same columns.  
DBResult.merge(DBResult result1) will merge result1 into the exist DBResult.  
```java
    DBResult result1 = client.query(query_1);
    DBResult result2 = client.query(query_2);
    DBResult result3 = client.query(query_3);
    if(result1.isSuccess())
    {
        System.out.println("size is "+ result1.getRowSize());
        result1.merge(result2,result3);
        System.out.println(result1.toJson());  
    }                 
```

## DBClient logs
DBClient use Log4j2.   
You need to define log4j2.xml in classpath to enable logging.    
There are some logs for debug.   
You can close it by add  
```xml
<Logger name="owlstone.db.DBConnectionManager" level="OFF" />
<Logger name="DBClient_PURE_CONSOLE" level="OFF" />
<Logger name="ConnectionInfo" level="OFF" />
```
to your log4j.xml.  

DBClient_PURE_CONSOLE is logger for print information on console.  
Logger 'ConnectionInfo' to trace the life and dead of connection.
So you can define a specific appender and assign to it to trace the life and dead of connection.  
```xml
<Appenders>
  <File name="CONN_APPENDER" fileName="logs/db_conn.log">
    <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %logger{36} %L %M - %msg%n"/>
  </File>
</Appenders>
<loggers>
    <logger name="ConnectionInfo" level="trace" additivity="false">
        <AppenderRef ref="CONN_APPENDER"/>     
    </logger>
<loggers>
```

## DBClient delta function Usage
Delta function used to simulate delta operation of DB.  

DBClient.deltaStart(InputStream is):DeltaJobInfo  
DBClient.deltaStart(InputStream is,JobFinishedCallBack callBack):DeltaJobInfo  
DBClient.deltaStop(String name):DeltaJobInfo  


### 1. write file dbclient-config.yaml
### 2. write log4j2.xml
### 3. write a delta job config file with any name in yaml format. e.g. delta-data.yaml
```yaml
# Simulate delta data to insert
#
# name : required
# the job name, you can call DBClient.deltaStop(name) to stop a running job
#
# zoneId : optional
# valid zoneId value can reference to https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html
# UTC is also valid value in zoneId.
# If zoneId is not provided, it will get your system time zone as zoneId
#
# ds: required
# the data source name in dbselect-config.yaml, we use this to get connection from DB
#
# table: required
# the table name
#
# number: required
# how many rows will be inserted to table.
#
# duration: required
# every 'duration' second execute one time.
# after execute 'number' times, job will stopped.
# min value of duration is 0.01
#
# columnStr: required
# the table columns
#
# values: required
# the value you want to insert to table, the number of values must equals to the number of columnStr
#
# You can specify dynamic variable in values
# dynamic value always starts with $
#
# $PK_INT to generate a primary key with integer type start from 1
# $PK_INT(5) to generate a primary key with integer type start from 6
#
# $VAR_INT to generate a random integer
# $VAR_INT+ to generate a random positive integer
# $VAR_INT|-1, 2, 3| to generate a integer -1, 2 or 3
# $VAR_INT|1~10| to generate a random integer from 1 to 9
# $VAR_INT|8| is invalid, if you want to generate number 8, just put 8 in value
#
#
# $VAR_FLT to generate a random float
# $VAR_FLT+ to generate a random positive float
# $VAR_FLT|1.1,2.7,3.2| to generate a float 1.1, 2.7 or 3.2
# $VAR_FLT|1.0| is invalid, if you want to generate number 1.0, just put 1.0 in value
#
#
# $VAR_STR to generate a random string with 5 characters
# $VAR_STR(7) to generate a random string with 7 characters
# $VAR_STR|A,BB,CCC| to generate a string A, BB, or CCC
#
# $VAR_TIME to generate a random time
# $VAR_TIME|2017-01-04T12:12:12~now|  to generate a random time from 2017-01-04T12:12:12 to now
# $VAR_TIME|now-5s~now|               to generate a random time from 5 seconds from now to now
# $VAR_TIME|now-5m~now|               to generate a random time from 5 minutes from now to now
# $VAR_TIME|now-5h~now|               to generate a random time from 5 hours from now to now
# $VAR_TIME|now-5d~now|               to generate a random time from 5 days from now to now
# $VAR_TIME|2017-1-04T12:12:12~now|   is invalid, month must have 2 digit
# $VAR_TIME|2017-01-4T12:12:12~now|   is invalid, day must have 2 digit
#
# $VAR_SQL|SELECT CustomerNumber FROM Customer.dbo.NewEggCustomer WHERE LoginTimer > '2017-02-22 20:43:50'|
#
#
!delta-data
name: test
zoneId: PST
ds: DB1
table: dbo.Customer
number: 20
duration: 30
columnStr: Id, FirstName, LastName, Age
values: ['$PK_INT', '$VAR_STR', '$VAR_STR|Jordan,Obama,Jackson,Durden,Bush|', '$VAR_INT|30-50|']
```
### 3. put dbclient-config.yaml and log4j2.xml location on java -cp
### 4. Sample Code
```java
    final Logger log = LogManager.getLogger(DeltaTest.class);
    final String DELTA_FILE_CUST_1 ="delta-data-cut01.yaml";
    final String DELTA_FILE_CUST_2 ="delta-data-cut02.yaml";
    final String DELTA_FILE_LOGIN ="delta-data-login.yaml";

    final String TABLE_CUT = "[master].[dbo].[MyCompanyUser]";
    final String TABLE_LOGIN = "[master].[dbo].[MyCompanyLogin]";

	DBConnectionManager dbMag = new DBConnectionManager();
    DBClient dbClient = new DBClient(dbMag);
    InputStream is_cut = null;
    final CountDownLatch exit = new CountDownLatch(1);
 
	try {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		is_cut = loader.getResource(DELTA_FILE_CUST_2).openStream();
	} catch (IOException e) {Assert.assertNull(e);}
	 
	// start delta job
	jobInfo = dbClient.deltaStart(is_cut, new JobFinishedCallBack() {
		@Override
		public void invoke() {
			log.debug("Table Customer insert job finished ");
			DBResult dbResult = dbClient.query(new Query(DB1,"SELECT * FROM "+TABLE_CUT));
			log.debug(dbResult.getRow(0);

			exit.countDown();
		}
	});

	Assert.assertTrue(jobInfo.isRunning());
	//wait for callback to be invoked
	while (true)
	{
		try
		{
			exit.await();
			break;
		}
		catch (InterruptedException e) {}
	}
```