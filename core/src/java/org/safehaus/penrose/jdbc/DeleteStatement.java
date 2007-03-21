package org.safehaus.penrose.jdbc;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatement extends Statement {

    protected String table;
    protected String whereClause;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String getSql() {

        log.debug("Generating delete statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("delete from ");

        sb.append(table);

        if (whereClause != null) {
            sb.append(" where ");
            sb.append(whereClause);
        }

        return sb.toString();
    }
}
