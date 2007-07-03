package org.safehaus.penrose.sql;

/**
 * @author Endi S. Dewata
 */
public class SQLSelect {

    String string;
    SQLTableList tableList;
    SQLWhere where;

    public SQLSelect() {
    }

    public void setString(String string) {
        this.string = string;
    }

    public String getString() {
        return string;
    }
    
    public String toString() {
        return string;
    }

    public SQLWhere getWhere() {
        return where;
    }

    public void setWhere(SQLWhere where) {
        this.where = where;
    }

    public SQLTableList getTableList() {
        return tableList;
    }

    public void setTableList(SQLTableList tableList) {
        this.tableList = tableList;
    }
}
