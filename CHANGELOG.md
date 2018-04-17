### [ v1.1.2 ] - 2018-04-12
- fix bug of value format not consistent when column type is  TimeStamp of PStmt

### [ v1.1.1 ] - 2018-04-10
- fix bug of value format not consistent when column type is  TimeStamp of Query  

### [ v1.1.0 ] - 2018-04-09
- add generatedPKList parameter to DBResult
- update DBClient alter() and prepareStatement() to get generated PKs. 

### [ v1.0.0 ] - 2018-03-29
- New strategy of keeping DB connections.  
- Remove DBConnectionManager singleton pattern.    
- Add support of PrepareStatement.
- Integrate DBClient action methods to one method execute.