package org.safehaus.penrose.sql;

/**
 * @author Endi S. Dewata
 */
public class SQLTableRef {

    private String tableName;
    private String aliasName;

    public String getTableName() {
        return tableName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName);

        if (aliasName != null) {
            sb.append(' ');
            sb.append(aliasName);
        }

        return sb.toString();
    }
}
