package owlstone.dbclient.db.module;

public enum DBType {
    MySQL("com.mysql.jdbc.Driver",'`','`'),
    SQLServer("com.microsoft.sqlserver.jdbc.SQLServerDriver",'[',']'),
    MariaDB("org.mariadb.jdbc.Driver",'`','`');

    private String jdbcDriver;
    private char[] escapeChar = new char[2];

    DBType(String jdbcDriver,char beginEscape,char endEscape){
        this.jdbcDriver = jdbcDriver;
        this.escapeChar[0] = beginEscape;
        this.escapeChar[1] = endEscape;
    }

    public String getJdbcDriver(){
        return jdbcDriver;
    }

    public char[] getEscapeChar() {
        return escapeChar;
    }
}
